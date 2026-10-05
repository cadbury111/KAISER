"""KAISER — Market Data Quality Monitor & Anomaly Detection Foundation.

Implements a production-grade, thread-safe, bounded data quality monitor and
deterministic anomaly detection engine for streaming and historical market data:
1. Evaluates all 8 quality dimensions:
   - Completeness (missing bars, timeframe intervals, observations)
   - Freshness (last observed timestamp, stale threshold, injected reference time)
   - Chronology (out-of-order, monotonicity, duplicate timestamps, UTC consistency)
   - OHLC Integrity (high >= max(O,C,L), low <= min(O,C,H), high >= low, positive prices, volume >= 0)
   - Numerical Validity (rejects NaN, +Inf, -Inf, non-finite timestamps; preserves valid zeros)
   - Duplication (distinguishes harmless identical duplicates from conflicting duplicates)
   - Gap Detection (expected interval gaps, start/end, duration, missed observation counts)
   - Cross-Record Consistency (symbol and timeframe consistency, metadata validation)
2. Rule-based deterministic anomaly detection (price jump, volume spike, zero volume,
   large candle range, repeated identical bars, excessive gaps).
3. Structured severity model and bounded quality score in [0.0, 100.0].
4. Bounded StateStore and EventBus integrations.
"""

from __future__ import annotations

import collections
import copy
from dataclasses import asdict
from datetime import datetime, timezone
import math
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union
import uuid

from app.analysis.models import SUPPORTED_TIMEFRAMES, TIMEFRAME_SECONDS, normalize_timeframe
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.models import Bar
from app.market.normalizer import parse_timestamp
from app.market.quality.models import (
    AnomalyType,
    MarketAnomaly,
    QualityFinding,
    QualitySeverity,
    max_severity,
    sanitize_message,
)
from app.market.quality.policy import (
    MarketQualityConfig,
    calculate_quality_score,
    is_safe_for_downstream,
    resolve_quality_status,
)
from app.market.quality.status import DataQualityAssessment, DataQualityStatus
from app.market.validators import DataFreshnessStatus, check_freshness

logger = get_logger("market.quality.monitor")


def _to_epoch(ts: Union[float, int, datetime]) -> float:
    """Convert timestamp or datetime to epoch float safely."""
    if isinstance(ts, datetime):
        return ts.timestamp()
    return float(ts)


class MarketDataQualityMonitor:
    """Production-grade market data reliability monitoring and anomaly detection engine."""

    def __init__(
        self,
        config: Optional[MarketQualityConfig] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "market_data_quality_monitor",
    ) -> None:
        self.name = name
        self.config = config or MarketQualityConfig()
        self.event_bus = event_bus
        self.state_store = state_store
        self._lock = threading.RLock()

        # Bounded in-memory assessment cache: (symbol, timeframe) -> DataQualityAssessment
        self._latest_assessments: Dict[Tuple[str, str], DataQualityAssessment] = {}

        # Bounded in-memory anomaly ring buffer
        self._anomalies: collections.deque[MarketAnomaly] = collections.deque(
            maxlen=self.config.max_stored_anomalies
        )

        # Counter metrics
        self._total_evaluations: int = 0
        self._total_anomalies_detected: int = 0

    @property
    def total_evaluations(self) -> int:
        """Return total evaluation runs performed."""
        with self._lock:
            return self._total_evaluations

    @property
    def total_anomalies_detected(self) -> int:
        """Return total anomaly instances detected."""
        with self._lock:
            return self._total_anomalies_detected

    def evaluate_bar_series(
        self,
        bars: Sequence[Union[Bar, Dict[str, Any]]],
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        source: Optional[str] = None,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> DataQualityAssessment:
        """Execute a comprehensive, single-pass quality and anomaly assessment across a bar series.

        Args:
            bars: Sequence of Bar instances or raw OHLCV dictionaries.
            symbol: Expected ticker symbol (optional if inferrable from bars).
            timeframe: Expected bar timeframe (e.g. '15m').
            source: Provenance data source tag.
            reference_time: Reference POSIX epoch float for deterministic clock injection.
            correlation_id: Tracing correlation ID.

        Returns:
            DataQualityAssessment: Structured, immutable audit result.
        """
        now = time.time() if reference_time is None else _to_epoch(reference_time)
        cid = correlation_id or uuid.uuid4().hex
        assessment_id = f"dqa_{uuid.uuid4().hex[:12]}"

        # Resolve primary symbol and timeframe
        norm_sym = symbol.strip().upper() if symbol else ""
        norm_tf = ""
        if timeframe:
            try:
                norm_tf = normalize_timeframe(timeframe)
            except ValidationError:
                norm_tf = timeframe.strip()

        # Infer source from first record if not explicitly specified
        if not source and bars:
            first_b = bars[0]
            if isinstance(first_b, Bar):
                src_tag = first_b.source
            elif isinstance(first_b, (dict, collections.abc.Mapping)):
                src_tag = str(first_b.get("source", "market_data"))
            else:
                src_tag = "market_data"
        else:
            src_tag = source or "market_data"

        # 1. Handle Empty Input
        if not bars:
            findings = (
                QualityFinding(
                    finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                    dimension="completeness",
                    rule_name="empty_dataset_check",
                    severity=QualitySeverity.ERROR,
                    message="Market data series is completely empty; zero records observed.",
                    affected_count=0,
                ),
            )
            assessment = DataQualityAssessment(
                assessment_id=assessment_id,
                symbol=norm_sym or "UNKNOWN",
                data_type="series",
                source=src_tag,
                timestamp=0.0,
                quality_status=DataQualityStatus.MISSING,
                checks_performed=("empty_dataset_check",),
                warnings=(),
                errors=("Dataset contains zero observations",),
                freshness_status=DataFreshnessStatus.INVALID,
                completeness_status="INCOMPLETE",
                sequence_status="UNKNOWN",
                duplicate_status="UNIQUE",
                assessment_timestamp=now,
                correlation_id=cid,
                safe_to_consume=False,
                timeframe=norm_tf or "UNKNOWN",
                evaluated_at=now,
                data_start=None,
                data_end=None,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                duplicate_count=0,
                conflicting_duplicate_count=0,
                out_of_order_count=0,
                gap_count=0,
                stale_status=True,
                anomaly_count=0,
                highest_severity=QualitySeverity.ERROR,
                findings=findings,
                anomalies=(),
                provenance={"source": src_tag, "empty": True},
                quality_score=0.0,
            )
            self._record_assessment(assessment)
            return assessment

        # Infer symbol/timeframe from first record if missing
        first_item = bars[0]
        if not norm_sym:
            if isinstance(first_item, Bar):
                norm_sym = first_item.symbol
            elif isinstance(first_item, (dict, collections.abc.Mapping)) and "symbol" in first_item:
                norm_sym = str(first_item["symbol"]).strip().upper()
            else:
                norm_sym = "UNKNOWN"

        if not norm_tf:
            if isinstance(first_item, Bar):
                norm_tf = first_item.timeframe
            elif isinstance(first_item, (dict, collections.abc.Mapping)) and "timeframe" in first_item:
                norm_tf = str(first_item["timeframe"]).strip()
            else:
                norm_tf = "15m"

        try:
            canon_tf = normalize_timeframe(norm_tf)
        except ValidationError:
            canon_tf = norm_tf

        interval_seconds = TIMEFRAME_SECONDS.get(canon_tf, 60.0)

        # Tracking state
        total_records = len(bars)
        valid_records = 0
        invalid_records = 0
        duplicate_count = 0
        conflicting_duplicate_count = 0
        out_of_order_count = 0
        gap_count = 0
        findings: List[QualityFinding] = []
        anomalies: List[MarketAnomaly] = []
        checks_performed: List[str] = [
            "completeness_check",
            "freshness_check",
            "chronology_check",
            "ohlc_integrity_check",
            "numerical_validity_check",
            "duplication_check",
            "gap_detection_check",
            "cross_record_consistency_check",
            "rule_based_anomaly_check",
        ]

        # Parsed clean bars for chronological and anomaly analysis: list of (Bar, raw_index)
        valid_bars: List[Bar] = []
        seen_timestamps: Dict[float, Bar] = {}

        # 2. Sequential Validation Pass across raw inputs
        prev_ts: Optional[float] = None

        for idx, item in enumerate(bars):
            # A & E & D: Numerical validity & OHLC integrity parsing
            bar: Optional[Bar] = None
            invalid_reason: Optional[str] = None

            if isinstance(item, Bar):
                bar = item
            elif isinstance(item, (dict, collections.abc.Mapping)):
                # Validate raw dictionary representation
                try:
                    ts_val = item.get("timestamp")
                    if ts_val is None:
                        raise ValidationError(f"Record #{idx} missing timestamp.")
                    ts_float = parse_timestamp(ts_val)
                    if not math.isfinite(ts_float) or ts_float <= 0:
                        raise ValidationError(f"Record #{idx} timestamp must be a positive finite float, got {ts_val}")

                    sym_val = str(item.get("symbol", norm_sym)).strip().upper()
                    tf_val = str(item.get("timeframe", canon_tf)).strip()
                    exch_val = str(item.get("exchange", "NSE")).strip().upper()
                    src_val = str(item.get("source", src_tag)).strip()

                    # Numerical price validations
                    for p_field in ("open", "high", "low", "close"):
                        val = item.get(p_field)
                        if val is None:
                            raise ValidationError(f"Record #{idx} missing required field '{p_field}'")
                        f_val = float(val)
                        if not math.isfinite(f_val) or f_val < 0:
                            raise ValidationError(f"Record #{idx} field '{p_field}' must be non-negative finite float, got {val}")

                    vol_val = item.get("volume", 0.0)
                    f_vol = float(vol_val)
                    if not math.isfinite(f_vol) or f_vol < 0:
                        raise ValidationError(f"Record #{idx} volume must be non-negative finite float, got {vol_val}")

                    o_val = float(item["open"])
                    h_val = float(item["high"])
                    l_val = float(item["low"])
                    c_val = float(item["close"])

                    # Relational OHLC checks
                    if h_val < o_val or h_val < c_val or h_val < l_val:
                        raise ValidationError(f"Record #{idx} inverted high: {h_val} < max({o_val}, {c_val}, {l_val})")
                    if l_val > o_val or l_val > c_val or l_val > h_val:
                        raise ValidationError(f"Record #{idx} inverted low: {l_val} > min({o_val}, {c_val}, {h_val})")

                    bar = Bar(
                        symbol=sym_val,
                        exchange=exch_val,
                        timeframe=tf_val,
                        timestamp=ts_float,
                        open=o_val,
                        high=h_val,
                        low=l_val,
                        close=c_val,
                        volume=f_vol,
                        source=src_val,
                    )
                except (ValidationError, ValueError, TypeError) as exc:
                    invalid_reason = str(exc)
            else:
                invalid_reason = f"Unsupported record type: {type(item).__name__}"

            if invalid_reason or bar is None:
                invalid_records += 1
                findings.append(
                    QualityFinding(
                        finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                        dimension="numerical_validity",
                        rule_name="structural_and_numerical_invariants",
                        severity=QualitySeverity.ERROR,
                        message=f"Corrupt or invalid bar at index #{idx}: {invalid_reason}",
                        affected_count=1,
                        details={"index": idx, "reason": invalid_reason},
                    )
                )
                anomalies.append(
                    MarketAnomaly(
                        anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                        symbol=norm_sym,
                        timeframe=canon_tf,
                        start_timestamp=0.0,
                        anomaly_type=AnomalyType.NON_FINITE_VALUE if "non-finite" in invalid_reason.lower() or "nan" in invalid_reason.lower() else AnomalyType.INVERTED_OHLC,
                        severity=QualitySeverity.ERROR,
                        observed_value=str(item)[:100],
                        reason=invalid_reason,
                        provenance=src_tag,
                        detected_at=now,
                    )
                )
                continue

            # H: Cross-record consistency check
            if bar.symbol.strip().upper() != norm_sym:
                invalid_records += 1
                msg = f"Symbol mismatch at #{idx}: expected '{norm_sym}', got '{bar.symbol}'"
                findings.append(
                    QualityFinding(
                        finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                        dimension="cross_record_consistency",
                        rule_name="symbol_uniformity",
                        severity=QualitySeverity.ERROR,
                        message=msg,
                        affected_count=1,
                        timestamp=bar.timestamp,
                    )
                )
                anomalies.append(
                    MarketAnomaly(
                        anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                        symbol=bar.symbol,
                        timeframe=canon_tf,
                        start_timestamp=bar.timestamp,
                        anomaly_type=AnomalyType.SYMBOL_MISMATCH,
                        severity=QualitySeverity.ERROR,
                        observed_value=bar.symbol,
                        threshold_or_reference=norm_sym,
                        reason=msg,
                        provenance=src_tag,
                        detected_at=now,
                    )
                )
                continue

            # C & F: Chronology, Sequence & Duplication
            if prev_ts is not None:
                if bar.timestamp < prev_ts:
                    out_of_order_count += 1
                    findings.append(
                        QualityFinding(
                            finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                            dimension="chronology",
                            rule_name="timestamp_monotonicity",
                            severity=QualitySeverity.WARNING,
                            message=f"Out-of-order timestamp at #{idx}: {bar.timestamp:.2f} < previous {prev_ts:.2f}",
                            affected_count=1,
                            timestamp=bar.timestamp,
                            details={"timestamp": bar.timestamp, "previous_timestamp": prev_ts},
                        )
                    )
                    anomalies.append(
                        MarketAnomaly(
                            anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                            symbol=norm_sym,
                            timeframe=canon_tf,
                            start_timestamp=bar.timestamp,
                            anomaly_type=AnomalyType.OUT_OF_ORDER_TIMESTAMP,
                            severity=QualitySeverity.WARNING,
                            observed_value=bar.timestamp,
                            threshold_or_reference=prev_ts,
                            reason=f"Timestamp {bar.timestamp:.2f} is earlier than predecessor {prev_ts:.2f}",
                            provenance=src_tag,
                            detected_at=now,
                        )
                    )
                elif bar.timestamp == prev_ts:
                    # Duplicate timestamp: distinguish harmless from conflicting
                    prev_bar = seen_timestamps.get(bar.timestamp)
                    is_conflicting = False
                    if prev_bar is not None:
                        is_conflicting = (
                            prev_bar.open != bar.open
                            or prev_bar.high != bar.high
                            or prev_bar.low != bar.low
                            or prev_bar.close != bar.close
                            or prev_bar.volume != bar.volume
                        )

                    if is_conflicting:
                        conflicting_duplicate_count += 1
                        msg = f"Conflicting duplicate record at timestamp {bar.timestamp}: differing OHLCV values observed."
                        findings.append(
                            QualityFinding(
                                finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                                dimension="duplication",
                                rule_name="conflicting_duplicate_check",
                                severity=QualitySeverity.ERROR,
                                message=msg,
                                affected_count=1,
                                timestamp=bar.timestamp,
                            )
                        )
                        anomalies.append(
                            MarketAnomaly(
                                anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                                symbol=norm_sym,
                                timeframe=canon_tf,
                                start_timestamp=bar.timestamp,
                                anomaly_type=AnomalyType.CONFLICTING_DUPLICATE,
                                severity=QualitySeverity.ERROR,
                                observed_value=bar.to_dict(),
                                threshold_or_reference=prev_bar.to_dict() if prev_bar else None,
                                reason=msg,
                                provenance=src_tag,
                                detected_at=now,
                            )
                        )
                    else:
                        duplicate_count += 1
                        findings.append(
                            QualityFinding(
                                finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                                dimension="duplication",
                                rule_name="harmless_duplicate_check",
                                severity=QualitySeverity.INFO,
                                message=f"Harmless identical duplicate record at timestamp {bar.timestamp}",
                                affected_count=1,
                                timestamp=bar.timestamp,
                            )
                        )

            prev_ts = bar.timestamp
            seen_timestamps[bar.timestamp] = bar
            valid_records += 1
            valid_bars.append(bar)

        # 3. Chronological Gap Detection on Deduplicated, Sorted Timeline
        # Sort valid bars by timestamp strictly to identify actual timeline gaps
        sorted_bars = sorted(valid_bars, key=lambda b: b.timestamp)
        data_start: Optional[float] = sorted_bars[0].timestamp if sorted_bars else None
        data_end: Optional[float] = sorted_bars[-1].timestamp if sorted_bars else None

        for i in range(len(sorted_bars) - 1):
            t_curr = sorted_bars[i].timestamp
            t_next = sorted_bars[i + 1].timestamp
            diff = t_next - t_curr

            if diff > interval_seconds * 1.5:
                missed = max(1, int(round(diff / interval_seconds)) - 1)
                gap_count += 1
                is_excessive = (
                    diff > self.config.max_allowed_gap_seconds
                    or missed > (self.config.excessive_gap_multiplier * 5)
                )
                gap_sev = QualitySeverity.ERROR if is_excessive else QualitySeverity.WARNING
                msg = (
                    f"Timeline gap of {diff:.1f}s ({missed} missing bars) between "
                    f"{t_curr:.0f} and {t_next:.0f}"
                )
                findings.append(
                    QualityFinding(
                        finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                        dimension="gap_detection",
                        rule_name="expected_interval_gaps",
                        severity=gap_sev,
                        message=msg,
                        affected_count=missed,
                        timestamp=t_curr,
                        details={
                            "from_timestamp": t_curr,
                            "to_timestamp": t_next,
                            "gap_seconds": diff,
                            "missed_bars": missed,
                        },
                    )
                )
                if is_excessive:
                    anomalies.append(
                        MarketAnomaly(
                            anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                            symbol=norm_sym,
                            timeframe=canon_tf,
                            start_timestamp=t_curr,
                            end_timestamp=t_next,
                            anomaly_type=AnomalyType.EXCESSIVE_GAP,
                            severity=QualitySeverity.ERROR,
                            observed_value=diff,
                            threshold_or_reference=self.config.max_allowed_gap_seconds,
                            reason=msg,
                            provenance=src_tag,
                            detected_at=now,
                        )
                    )

        # 4. Freshness & Injected Clock Verification
        stale_status = False
        freshness_enum = DataFreshnessStatus.FRESH

        # Reference time resolution:
        # 1. If reference_time is explicitly passed, use it deterministically.
        # 2. If reference_time is None:
        #    - If source is a live/realtime stream, use current system wall-clock time `now`.
        #    - For offline/historical/test/batch/replay/mock/orchestrator datasets, default
        #      reference_time to data_end (or `now` if empty) to avoid false staleness on
        #      historical/offline data (as mandated by Step 19 specification).
        effective_now = now
        if reference_time is not None:
            effective_now = _to_epoch(reference_time)
        elif data_end is not None:
            live_sources = {"live", "realtime", "stream", "ws", "websocket"}
            if src_tag.lower() not in live_sources:
                effective_now = data_end

        if data_end is not None:
            # Check for impossible future timestamp (clock skew > 5s)
            if (data_end - effective_now) > 5.0:
                freshness_enum = DataFreshnessStatus.INVALID
                msg = f"Clock skew defect: latest bar timestamp {data_end:.0f} is ahead of reference time {effective_now:.0f}"
                findings.append(
                    QualityFinding(
                        finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                        dimension="freshness",
                        rule_name="future_timestamp_skew",
                        severity=QualitySeverity.ERROR,
                        message=msg,
                        affected_count=1,
                        timestamp=data_end,
                    )
                )
            else:
                age_seconds = effective_now - data_end
                if age_seconds > self.config.stale_threshold_seconds:
                    stale_status = True
                    freshness_enum = DataFreshnessStatus.STALE
                    findings.append(
                        QualityFinding(
                            finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                            dimension="freshness",
                            rule_name="stale_data_threshold",
                            severity=QualitySeverity.WARNING,
                            message=f"Market data is stale: age {age_seconds:.1f}s exceeds threshold {self.config.stale_threshold_seconds}s",
                            affected_count=1,
                            timestamp=data_end,
                            details={"age_seconds": age_seconds, "threshold": self.config.stale_threshold_seconds},
                        )
                    )

        # 5. Rule-Based Anomaly Detection on Sorted Chronological Sequence
        # A: Price Jump & Large Range & Zero Volume & Repeated Bars
        rolling_volumes: List[float] = []
        consecutive_identical_count = 1

        for i, b in enumerate(sorted_bars):
            # Zero-volume check
            if b.volume == 0.0:
                findings.append(
                    QualityFinding(
                        finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                        dimension="anomaly",
                        rule_name="zero_volume_observation",
                        severity=QualitySeverity.INFO,
                        message=f"Bar at {b.timestamp:.0f} reported zero volume.",
                        affected_count=1,
                        timestamp=b.timestamp,
                    )
                )
                anomalies.append(
                    MarketAnomaly(
                        anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                        symbol=norm_sym,
                        timeframe=canon_tf,
                        start_timestamp=b.timestamp,
                        anomaly_type=AnomalyType.ZERO_VOLUME,
                        severity=QualitySeverity.INFO,
                        observed_value=0.0,
                        threshold_or_reference=0.0,
                        reason="Zero volume observation on market bar.",
                        provenance=src_tag,
                        detected_at=now,
                    )
                )

            # Large Candle Range Check
            if b.open > 0:
                range_pct = (b.high - b.low) / b.open
                if range_pct > self.config.large_range_threshold_pct:
                    sev = QualitySeverity.ERROR if range_pct > (self.config.large_range_threshold_pct * 2) else QualitySeverity.WARNING
                    msg = f"Unusually large candle range {range_pct * 100.0:.2f}% exceeds threshold {self.config.large_range_threshold_pct * 100.0:.1f}%"
                    findings.append(
                        QualityFinding(
                            finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                            dimension="anomaly",
                            rule_name="large_candle_range",
                            severity=sev,
                            message=msg,
                            affected_count=1,
                            timestamp=b.timestamp,
                        )
                    )
                    anomalies.append(
                        MarketAnomaly(
                            anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                            symbol=norm_sym,
                            timeframe=canon_tf,
                            start_timestamp=b.timestamp,
                            anomaly_type=AnomalyType.LARGE_CANDLE_RANGE,
                            severity=sev,
                            observed_value=round(range_pct, 4),
                            threshold_or_reference=self.config.large_range_threshold_pct,
                            reason=msg,
                            provenance=src_tag,
                            detected_at=now,
                        )
                    )

            # Consecutive bar comparisons
            if i > 0:
                prev_b = sorted_bars[i - 1]

                # Price jump check
                if prev_b.close > 0:
                    jump_pct = abs(b.close - prev_b.close) / prev_b.close
                    if jump_pct > self.config.price_jump_threshold_pct:
                        sev = QualitySeverity.ERROR if jump_pct > (self.config.price_jump_threshold_pct * 2) else QualitySeverity.WARNING
                        msg = f"Abnormal price jump of {jump_pct * 100.0:.2f}% between {prev_b.close} and {b.close}"
                        findings.append(
                            QualityFinding(
                                finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                                dimension="anomaly",
                                rule_name="abnormal_price_jump",
                                severity=sev,
                                message=msg,
                                affected_count=1,
                                timestamp=b.timestamp,
                            )
                        )
                        anomalies.append(
                            MarketAnomaly(
                                anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                                symbol=norm_sym,
                                timeframe=canon_tf,
                                start_timestamp=b.timestamp,
                                anomaly_type=AnomalyType.PRICE_JUMP,
                                severity=sev,
                                observed_value=round(jump_pct, 4),
                                threshold_or_reference=self.config.price_jump_threshold_pct,
                                reason=msg,
                                provenance=src_tag,
                                detected_at=now,
                            )
                        )

                # Volume spike check
                if rolling_volumes and len(rolling_volumes) >= 5:
                    avg_vol = sum(rolling_volumes[-20:]) / len(rolling_volumes[-20:])
                    if avg_vol > 0 and b.volume > (avg_vol * self.config.volume_spike_multiplier):
                        msg = f"Volume spike {b.volume:.0f} exceeds {self.config.volume_spike_multiplier}x average {avg_vol:.0f}"
                        findings.append(
                            QualityFinding(
                                finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                                dimension="anomaly",
                                rule_name="volume_spike_detection",
                                severity=QualitySeverity.WARNING,
                                message=msg,
                                affected_count=1,
                                timestamp=b.timestamp,
                            )
                        )
                        anomalies.append(
                            MarketAnomaly(
                                anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                                symbol=norm_sym,
                                timeframe=canon_tf,
                                start_timestamp=b.timestamp,
                                anomaly_type=AnomalyType.VOLUME_SPIKE,
                                severity=QualitySeverity.WARNING,
                                observed_value=round(b.volume, 2),
                                threshold_or_reference=round(avg_vol, 2),
                                reason=msg,
                                provenance=src_tag,
                                detected_at=now,
                            )
                        )

                # Repeated identical bars check
                if (
                    b.open == prev_b.open
                    and b.high == prev_b.high
                    and b.low == prev_b.low
                    and b.close == prev_b.close
                    and b.volume == prev_b.volume
                ):
                    consecutive_identical_count += 1
                    if consecutive_identical_count >= self.config.repeated_identical_bars_threshold:
                        msg = f"Suspiciously repeated identical bars: {consecutive_identical_count} consecutive identical candles."
                        findings.append(
                            QualityFinding(
                                finding_id=f"fnd_{uuid.uuid4().hex[:8]}",
                                dimension="anomaly",
                                rule_name="repeated_identical_bars",
                                severity=QualitySeverity.WARNING,
                                message=msg,
                                affected_count=consecutive_identical_count,
                                timestamp=b.timestamp,
                            )
                        )
                        anomalies.append(
                            MarketAnomaly(
                                anomaly_id=f"anom_{uuid.uuid4().hex[:8]}",
                                symbol=norm_sym,
                                timeframe=canon_tf,
                                start_timestamp=b.timestamp,
                                anomaly_type=AnomalyType.REPEATED_IDENTICAL_BARS,
                                severity=QualitySeverity.WARNING,
                                observed_value=consecutive_identical_count,
                                threshold_or_reference=self.config.repeated_identical_bars_threshold,
                                reason=msg,
                                provenance=src_tag,
                                detected_at=now,
                            )
                        )
                else:
                    consecutive_identical_count = 1

            rolling_volumes.append(b.volume)

        # 6. Synthesize Highest Severity & Quality Status
        highest_sev = QualitySeverity.INFO
        for f in findings:
            highest_sev = max_severity(highest_sev, f.severity)
        for a in anomalies:
            highest_sev = max_severity(highest_sev, a.severity)

        # Categorical DataQualityStatus Precedence:
        # INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID
        has_fatal_errors = (
            invalid_records > 0
            or conflicting_duplicate_count > 0
            or highest_sev in (QualitySeverity.CRITICAL, QualitySeverity.ERROR)
            or freshness_enum == DataFreshnessStatus.INVALID
        )

        overall_status = resolve_quality_status(
            has_errors=has_fatal_errors,
            freshness=freshness_enum,
            is_missing=(valid_records == 0),
            is_out_of_order=(out_of_order_count > 0),
            is_duplicate=(duplicate_count > 0),
            is_degraded=(highest_sev == QualitySeverity.WARNING or gap_count > 0 or len(findings) > 0),
        )

        safe = is_safe_for_downstream(overall_status)

        # 7. Calculate Deterministic Quality Score
        score = calculate_quality_score(
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicate_count=duplicate_count,
            conflicting_duplicate_count=conflicting_duplicate_count,
            out_of_order_count=out_of_order_count,
            gap_count=gap_count,
            is_stale=stale_status,
            anomalies=anomalies,
            status=overall_status,
        )

        # Legacy string lists for backward compatibility
        warning_strings = tuple(f.message for f in findings if f.severity in (QualitySeverity.WARNING, QualitySeverity.INFO))
        error_strings = tuple(f.message for f in findings if f.severity in (QualitySeverity.ERROR, QualitySeverity.CRITICAL))

        # Clamp findings to max bound to avoid unbounded memory
        bounded_findings = tuple(findings[: self.config.max_findings_per_assessment])
        bounded_anomalies = tuple(anomalies[: self.config.max_stored_anomalies])

        assessment = DataQualityAssessment(
            assessment_id=assessment_id,
            symbol=norm_sym,
            data_type="series",
            source=src_tag,
            timestamp=data_end or 0.0,
            quality_status=overall_status,
            checks_performed=tuple(checks_performed),
            warnings=warning_strings,
            errors=error_strings,
            freshness_status=freshness_enum,
            completeness_status="COMPLETE" if gap_count == 0 and valid_records > 0 else "PARTIAL",
            sequence_status="OUT_OF_ORDER" if out_of_order_count > 0 else "IN_ORDER",
            duplicate_status="DUPLICATE" if duplicate_count > 0 or conflicting_duplicate_count > 0 else "UNIQUE",
            assessment_timestamp=now,
            correlation_id=cid,
            safe_to_consume=safe,
            timeframe=canon_tf,
            evaluated_at=now,
            data_start=data_start,
            data_end=data_end,
            total_records=total_records,
            valid_records=valid_records,
            invalid_records=invalid_records,
            duplicate_count=duplicate_count,
            conflicting_duplicate_count=conflicting_duplicate_count,
            out_of_order_count=out_of_order_count,
            gap_count=gap_count,
            stale_status=stale_status,
            anomaly_count=len(anomalies),
            highest_severity=highest_sev,
            findings=bounded_findings,
            anomalies=bounded_anomalies,
            provenance={
                "source": src_tag,
                "evaluated_records": total_records,
                "correlation_id": cid,
            },
            quality_score=score,
        )

        # 8. Record and publish
        self._record_assessment(assessment)
        return assessment

    def _record_assessment(self, assessment: DataQualityAssessment) -> None:
        """Cache assessment, record anomalies, update StateStore, and publish events."""
        with self._lock:
            self._total_evaluations += 1
            key = (assessment.symbol, assessment.timeframe)
            self._latest_assessments[key] = assessment

            for anom in assessment.anomalies:
                self._anomalies.append(anom)
                self._total_anomalies_detected += 1

        # StateStore Integration
        self._update_state_store(assessment)

        # EventBus Integration
        self._publish_events(assessment)

    def _update_state_store(self, assessment: DataQualityAssessment) -> None:
        """Store bounded quality summary and latest assessments in StateStore."""
        if not self.state_store:
            return

        state_key = f"{assessment.symbol}_{assessment.timeframe.upper()}"
        self.state_store.update(
            "market_data_quality",
            {state_key: assessment.to_dict()},
            default={},
        )

        # Update high-level quality summary
        with self._lock:
            anom_list = [a.to_dict() for a in list(self._anomalies)[-50:]]
            summary = {
                "latest_assessment_id": assessment.assessment_id,
                "latest_symbol": assessment.symbol,
                "latest_timeframe": assessment.timeframe,
                "latest_status": assessment.quality_status.value,
                "latest_score": assessment.quality_score,
                "highest_severity": assessment.highest_severity.value,
                "total_evaluations": self._total_evaluations,
                "total_anomalies": self._total_anomalies_detected,
                "evaluated_at": assessment.evaluated_at,
            }

        self.state_store.set("data_quality_summary", summary)
        self.state_store.set("market_data_anomalies", anom_list)

    def _publish_events(self, assessment: DataQualityAssessment) -> None:
        """Publish structured, bounded quality and anomaly events to EventBus."""
        if not self.event_bus:
            return

        # Prepare bounded payload without sensitive fields
        payload = {
            "assessment_id": assessment.assessment_id,
            "symbol": assessment.symbol,
            "timeframe": assessment.timeframe,
            "source": assessment.source,
            "quality_status": assessment.quality_status.value,
            "quality_score": assessment.quality_score,
            "highest_severity": assessment.highest_severity.value,
            "total_records": assessment.total_records,
            "valid_records": assessment.valid_records,
            "invalid_records": assessment.invalid_records,
            "duplicate_count": assessment.duplicate_count,
            "conflicting_duplicate_count": assessment.conflicting_duplicate_count,
            "out_of_order_count": assessment.out_of_order_count,
            "gap_count": assessment.gap_count,
            "stale_status": assessment.stale_status,
            "anomaly_count": assessment.anomaly_count,
            "evaluated_at": assessment.evaluated_at,
            "safe_to_consume": assessment.safe_to_consume,
        }

        # 1. Primary quality assessed event
        self.event_bus.publish(
            Event.create(
                event_type=EventType.DATA_QUALITY_ASSESSED,
                source=self.name,
                correlation_id=assessment.correlation_id,
                payload=payload,
            )
        )
        self.event_bus.publish(
            Event.create(
                event_type=EventType.MARKET_DATA_QUALITY_UPDATED,
                source=self.name,
                correlation_id=assessment.correlation_id,
                payload=payload,
            )
        )

        # 2. Degraded event if status is not strictly VALID
        if assessment.quality_status in (
            DataQualityStatus.DEGRADED,
            DataQualityStatus.STALE,
            DataQualityStatus.INVALID,
            DataQualityStatus.MISSING,
        ):
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.DATA_QUALITY_DEGRADED,
                    source=self.name,
                    correlation_id=assessment.correlation_id,
                    payload=payload,
                )
            )

        # 3. Anomaly detected event if anomalies are present
        if assessment.anomalies:
            anom_payload = {
                "assessment_id": assessment.assessment_id,
                "symbol": assessment.symbol,
                "timeframe": assessment.timeframe,
                "anomaly_count": len(assessment.anomalies),
                "highest_severity": assessment.highest_severity.value,
                "anomalies": [a.to_dict() for a in assessment.anomalies[:10]],  # bounded top 10
            }
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.DATA_ANOMALY_DETECTED,
                    source=self.name,
                    correlation_id=assessment.correlation_id,
                    payload=anom_payload,
                )
            )

    def get_latest_assessment(
        self,
        symbol: str,
        timeframe: Optional[str] = None,
    ) -> Optional[DataQualityAssessment]:
        """Retrieve the latest cached quality assessment for a symbol and timeframe."""
        with self._lock:
            norm_sym = symbol.strip().upper()
            if timeframe:
                norm_tf = normalize_timeframe(timeframe)
                return self._latest_assessments.get((norm_sym, norm_tf))

            # Return latest assessment matching symbol
            matching = [
                a for (s, _), a in self._latest_assessments.items() if s == norm_sym
            ]
            if matching:
                return max(matching, key=lambda a: a.evaluated_at)
            return None

    def get_all_latest_assessments(self) -> Dict[str, Dict[str, Any]]:
        """Return snapshot dictionary of all latest stream assessments."""
        with self._lock:
            return {
                f"{k[0]}_{k[1]}": a.to_dict()
                for k, a in self._latest_assessments.items()
            }

    def get_anomalies(
        self,
        symbol: Optional[str] = None,
        severity: Optional[QualitySeverity] = None,
        limit: int = 50,
    ) -> List[MarketAnomaly]:
        """Query bounded in-memory anomaly history with optional filtering.

        Args:
            symbol: Optional symbol filter.
            severity: Optional minimum or matching severity filter.
            limit: Maximum count returned (capped at 200).
        """
        clamped_limit = max(1, min(200, limit))
        with self._lock:
            results: List[MarketAnomaly] = []
            norm_sym = symbol.strip().upper() if symbol else None

            # Iterate from newest to oldest
            for anom in reversed(self._anomalies):
                if norm_sym and anom.symbol != norm_sym:
                    continue
                if severity and anom.severity != severity and anom.severity < severity:
                    continue
                results.append(anom)
                if len(results) >= clamped_limit:
                    break

            return results

    def get_quality_summary(self) -> Dict[str, Any]:
        """Return a top-level, point-in-time operational quality summary."""
        with self._lock:
            latest = list(self._latest_assessments.values())
            valid_streams = sum(1 for a in latest if a.quality_status == DataQualityStatus.VALID)
            degraded_streams = sum(1 for a in latest if a.quality_status == DataQualityStatus.DEGRADED)
            stale_streams = sum(1 for a in latest if a.quality_status == DataQualityStatus.STALE)
            invalid_streams = sum(1 for a in latest if a.quality_status == DataQualityStatus.INVALID)

            return {
                "monitored_streams_count": len(self._latest_assessments),
                "valid_streams": valid_streams,
                "degraded_streams": degraded_streams,
                "stale_streams": stale_streams,
                "invalid_streams": invalid_streams,
                "total_evaluations": self._total_evaluations,
                "total_anomalies_detected": self._total_anomalies_detected,
                "active_anomalies_stored": len(self._anomalies),
            }

    def evaluate_stored_bars(
        self,
        repository: Any,
        symbol: str,
        timeframe: str,
        start_time: float,
        end_time: float,
        limit: int = 5000,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> DataQualityAssessment:
        """Query and evaluate stored historical bars from repository."""
        bars = repository.query_bars(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        last_bar_ts = bars[-1].timestamp if bars else end_time
        return self.evaluate_bar_series(
            bars=bars,
            symbol=symbol,
            timeframe=timeframe,
            source="sqlite_repository",
            reference_time=reference_time or last_bar_ts,
            correlation_id=correlation_id,
        )

    def evaluate_historical_loader(
        self,
        loader: Any,
        request: Any,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> DataQualityAssessment:
        """Fetch and evaluate historical bars using a HistoricalDataLoader."""
        response = loader.load_bars(request=request, correlation_id=correlation_id)
        last_bar_ts = response.bars[-1].timestamp if response.bars else request.end_time
        return self.evaluate_bar_series(
            bars=response.bars,
            symbol=request.symbol,
            timeframe=request.timeframe,
            source=f"loader_{response.provider_name}",
            reference_time=reference_time or last_bar_ts,
            correlation_id=correlation_id,
        )

    def evaluate_historical_replay(
        self,
        replay_result: Any,
        bars: Optional[Sequence[Bar]] = None,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> DataQualityAssessment:
        """Evaluate quality of replayed market data."""
        sym = replay_result.request.symbol
        tf = replay_result.request.timeframe
        last_bar_ts = bars[-1].timestamp if bars else (replay_result.end_timestamp or replay_result.request.end_time)
        return self.evaluate_bar_series(
            bars=bars or (),
            symbol=sym,
            timeframe=tf,
            source=f"replay_{replay_result.replay_id}",
            reference_time=reference_time or last_bar_ts,
            correlation_id=correlation_id or replay_result.replay_id,
        )

    def reset(self) -> None:
        """Reset internal caches and tracking state."""
        with self._lock:
            self._latest_assessments.clear()
            self._anomalies.clear()
            self._total_evaluations = 0
            self._total_anomalies_detected = 0
