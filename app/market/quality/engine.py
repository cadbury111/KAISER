"""KAISER — Market Data Quality Engine.

Evaluates normalized market data (Quote, Bar, OrderBookSnapshot) against data quality,
freshness, completeness, duplicate, out-of-order, and source-health invariants.
Dispatches quality assessments to the EventBus and caches them in the StateStore.
"""

from __future__ import annotations

import collections
import threading
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from app.core.events import Event, EventBus, EventType
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.models import Bar, OrderBookSnapshot, Quote
from app.market.quality.models import MarketAnomaly, QualitySeverity
from app.market.quality.monitor import MarketDataQualityMonitor
from app.market.quality.policy import (
    MarketQualityConfig,
    evaluate_bar_completeness,
    evaluate_order_book_completeness,
    evaluate_quote_completeness,
    is_safe_for_downstream,
    resolve_quality_status,
)
from app.market.quality.status import DataQualityAssessment, DataQualityStatus
from app.market.quality.tracker import SourceHealth, SourceHealthTracker
from app.market.validators import DataFreshnessStatus, check_freshness

logger = get_logger("market.quality")

MarketRecord = Union[Quote, Bar, OrderBookSnapshot]


class MarketDataQualityEngine:
    """Deterministic, thread-safe market data reliability evaluation engine."""

    def __init__(
        self,
        config: Optional[MarketQualityConfig] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
    ) -> None:
        self.config = config or MarketQualityConfig()
        self.event_bus = event_bus
        self.state_store = state_store
        self.source_tracker = SourceHealthTracker()
        self.monitor = MarketDataQualityMonitor(
            config=self.config,
            event_bus=self.event_bus,
            state_store=self.state_store,
        )
        self._lock = threading.RLock()

        # Bounded duplicate detection cache: map fingerprint -> timestamp
        self._seen_fingerprints: collections.OrderedDict[str, float] = collections.OrderedDict()

        # Out-of-order tracking: map (source, symbol, data_type) -> latest_accepted_timestamp
        self._latest_stream_timestamps: Dict[Tuple[str, str, str], float] = {}

    def _compute_fingerprint(self, record: MarketRecord, data_type: str) -> str:
        """Compute a deterministic, stable string fingerprint for duplicate detection."""
        if isinstance(record, Quote):
            return f"{record.source}:{record.symbol}:quote:{record.timestamp}:{record.last_price}:{record.volume}:{record.bid}:{record.ask}"
        elif isinstance(record, Bar):
            return f"{record.source}:{record.symbol}:bar:{record.timeframe}:{record.timestamp}:{record.open}:{record.high}:{record.low}:{record.close}:{record.volume}"
        elif isinstance(record, OrderBookSnapshot):
            return f"{record.source}:{record.symbol}:ob:{record.timestamp}:{record.best_bid}:{record.best_ask}:{len(record.bids)}:{len(record.asks)}"
        return f"{getattr(record, 'source', 'unk')}:{getattr(record, 'symbol', 'unk')}:{data_type}:{getattr(record, 'timestamp', 0)}"

    def evaluate_quote(
        self,
        quote: Quote,
        correlation_id: Optional[str] = None,
        reference_time: Optional[float] = None,
    ) -> DataQualityAssessment:
        """Evaluate data reliability for a Quote model."""
        return self._evaluate_record(
            record=quote,
            data_type="quote",
            correlation_id=correlation_id,
            reference_time=reference_time,
        )

    def evaluate_bar(
        self,
        bar: Bar,
        correlation_id: Optional[str] = None,
        reference_time: Optional[float] = None,
    ) -> DataQualityAssessment:
        """Evaluate data reliability for a Bar model."""
        return self._evaluate_record(
            record=bar,
            data_type="bar",
            correlation_id=correlation_id,
            reference_time=reference_time,
        )

    def evaluate_order_book(
        self,
        ob: OrderBookSnapshot,
        correlation_id: Optional[str] = None,
        reference_time: Optional[float] = None,
    ) -> DataQualityAssessment:
        """Evaluate data reliability for an OrderBookSnapshot model."""
        return self._evaluate_record(
            record=ob,
            data_type="order_book",
            correlation_id=correlation_id,
            reference_time=reference_time,
        )

    def _evaluate_record(
        self,
        record: MarketRecord,
        data_type: str,
        correlation_id: Optional[str] = None,
        reference_time: Optional[float] = None,
    ) -> DataQualityAssessment:
        """Internal worker executing all quality checks and precedence resolution."""
        cid = correlation_id or uuid.uuid4().hex
        now = time.time() if reference_time is None else reference_time
        checks_performed: List[str] = ["structural_invariants"]
        warnings: List[str] = []
        errors: List[str] = []

        symbol = record.symbol
        source = record.source
        rec_time = record.timestamp
        stream_key = (source, symbol, data_type)

        is_duplicate = False
        is_out_of_order = False

        with self._lock:
            # 1. Duplicate Detection
            if self.config.duplicate_detection_enabled:
                checks_performed.append("duplicate_check")
                fp = self._compute_fingerprint(record, data_type)
                if fp in self._seen_fingerprints:
                    is_duplicate = True
                    warnings.append(f"Duplicate record detected for {symbol} on stream {source}:{data_type}")
                else:
                    self._seen_fingerprints[fp] = rec_time
                    if len(self._seen_fingerprints) > self.config.duplicate_window_size:
                        self._seen_fingerprints.popitem(last=False)

            # 2. Out-of-Order Sequence Check
            if self.config.out_of_order_detection_enabled:
                checks_performed.append("sequence_check")
                latest_seen = self._latest_stream_timestamps.get(stream_key)
                if latest_seen is not None and rec_time < latest_seen:
                    is_out_of_order = True
                    warnings.append(
                        f"Out-of-order timestamp detected for {symbol}: record ({rec_time:.2f}) < latest seen ({latest_seen:.2f})"
                    )
                elif not is_duplicate:
                    self._latest_stream_timestamps[stream_key] = rec_time

        # 3. Freshness Check (Reusing Step 03 logic)
        checks_performed.append("freshness_check")
        freshness = check_freshness(
            rec_time,
            reference_time=now,
            threshold_seconds=self.config.freshness_threshold_seconds,
        )
        if freshness == DataFreshnessStatus.STALE:
            warnings.append(f"Data is stale: timestamp age exceeds {self.config.freshness_threshold_seconds}s")
        elif freshness == DataFreshnessStatus.INVALID:
            errors.append("Invalid timestamp value or impossible future clock skew")

        # 4. Completeness Check
        checks_performed.append("completeness_check")
        if isinstance(record, Quote):
            comp_status, comp_warnings = evaluate_quote_completeness(record)
        elif isinstance(record, Bar):
            comp_status, comp_warnings = evaluate_bar_completeness(record)
        elif isinstance(record, OrderBookSnapshot):
            comp_status, comp_warnings = evaluate_order_book_completeness(record)
        else:
            comp_status, comp_warnings = "COMPLETE", []

        warnings.extend(comp_warnings)
        is_degraded = (comp_status == "PARTIAL") or bool(warnings and not (is_duplicate or is_out_of_order or freshness != DataFreshnessStatus.FRESH))

        # 5. Deterministic Precedence Resolution
        # INVALID > MISSING > OUT_OF_ORDER > DUPLICATE > STALE > DEGRADED > VALID
        status = resolve_quality_status(
            has_errors=bool(errors),
            freshness=freshness,
            is_missing=False,
            is_out_of_order=is_out_of_order,
            is_duplicate=is_duplicate,
            is_degraded=is_degraded,
        )

        safe = is_safe_for_downstream(status)

        # 6. Source Health Tracking
        if self.config.source_health_enabled:
            self.source_tracker.record_observation(
                source=source,
                status=status,
                record_timestamp=rec_time,
                processing_time=now,
            )

        # 7. Construct Immutable Assessment
        assessment = DataQualityAssessment(
            assessment_id=uuid.uuid4().hex,
            symbol=symbol,
            data_type=data_type,
            source=source,
            timestamp=rec_time,
            quality_status=status,
            checks_performed=tuple(checks_performed),
            warnings=tuple(warnings),
            errors=tuple(errors),
            freshness_status=freshness,
            completeness_status=comp_status,
            sequence_status="OUT_OF_ORDER" if is_out_of_order else "IN_ORDER",
            duplicate_status="DUPLICATE" if is_duplicate else "UNIQUE",
            assessment_timestamp=now,
            correlation_id=cid,
            safe_to_consume=safe,
        )

        # 8. State Store Integration
        self._update_state_store(assessment)

        # 9. Event Bus Integration
        self._publish_events(assessment)

        return assessment

    def check_missing(
        self,
        symbol: str,
        source: str,
        data_type: str = "quote",
        timeout_seconds: Optional[float] = None,
        current_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> Optional[DataQualityAssessment]:
        """Verify if a stream has gone silent and missing beyond configured timeout."""
        now = time.time() if current_time is None else current_time
        max_wait = timeout_seconds or self.config.missing_data_timeout_seconds
        stream_key = (source, symbol, data_type)

        with self._lock:
            last_seen = self._latest_stream_timestamps.get(stream_key)

        is_missing = False
        if last_seen is None or (now - last_seen) > max_wait:
            is_missing = True

        if is_missing:
            cid = correlation_id or uuid.uuid4().hex
            assessment = DataQualityAssessment(
                assessment_id=uuid.uuid4().hex,
                symbol=symbol,
                data_type=data_type,
                source=source,
                timestamp=last_seen or 0.0,
                quality_status=DataQualityStatus.MISSING,
                checks_performed=("heartbeat_timeout_check",),
                warnings=(f"Market data stream {source}:{symbol}:{data_type} has not produced data within {max_wait}s",),
                errors=(),
                freshness_status=DataFreshnessStatus.INVALID if last_seen is None else DataFreshnessStatus.STALE,
                completeness_status="INCOMPLETE",
                sequence_status="UNKNOWN",
                duplicate_status="UNIQUE",
                assessment_timestamp=now,
                correlation_id=cid,
                safe_to_consume=False,
            )
            self._update_state_store(assessment)
            self._publish_events(assessment)
            return assessment

        return None

    def _update_state_store(self, assessment: DataQualityAssessment) -> None:
        """Update bounded operational state in StateStore."""
        if not self.state_store:
            return

        state_key = f"{assessment.symbol}_{assessment.data_type}"
        self.state_store.update(
            "market_data_quality",
            {state_key: assessment.to_dict()},
            default={},
        )
        if self.config.source_health_enabled:
            health = self.source_tracker.get_health(assessment.source)
            self.state_store.update(
                "market_source_health",
                {assessment.source: health.to_dict()},
                default={},
            )

    def _publish_events(self, assessment: DataQualityAssessment) -> None:
        """Publish quality events to EventBus without event storming."""
        if not self.event_bus:
            return

        payload = assessment.to_dict()

        # Primary quality event
        self.event_bus.publish(
            Event.create(
                event_type=EventType.MARKET_DATA_QUALITY_UPDATED,
                source="quality_engine",
                correlation_id=assessment.correlation_id,
                payload=payload,
            )
        )

        # Selective categorized event notification
        specific_event_type: Optional[EventType] = None
        if assessment.quality_status == DataQualityStatus.DEGRADED:
            specific_event_type = EventType.MARKET_DATA_DEGRADED
        elif assessment.quality_status == DataQualityStatus.INVALID:
            specific_event_type = EventType.MARKET_DATA_INVALID
        elif assessment.quality_status == DataQualityStatus.MISSING:
            specific_event_type = EventType.MARKET_DATA_MISSING
        elif assessment.quality_status == DataQualityStatus.DUPLICATE:
            specific_event_type = EventType.MARKET_DATA_DUPLICATE
        elif assessment.quality_status == DataQualityStatus.OUT_OF_ORDER:
            specific_event_type = EventType.MARKET_DATA_OUT_OF_ORDER

        if specific_event_type:
            self.event_bus.publish(
                Event.create(
                    event_type=specific_event_type,
                    source="quality_engine",
                    correlation_id=assessment.correlation_id,
                    payload=payload,
                )
            )

    def evaluate_bar_series(
        self,
        bars: Sequence[Union[Bar, Dict[str, Any]]],
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
        source: Optional[str] = None,
        reference_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
    ) -> DataQualityAssessment:
        """Evaluate a sequence of bars against quality dimensions and detect anomalies."""
        return self.monitor.evaluate_bar_series(
            bars=bars,
            symbol=symbol,
            timeframe=timeframe,
            source=source,
            reference_time=reference_time,
            correlation_id=correlation_id,
        )

    def get_latest_assessment(
        self,
        symbol: str,
        timeframe: Optional[str] = None,
    ) -> Optional[DataQualityAssessment]:
        """Retrieve latest assessment from monitor."""
        return self.monitor.get_latest_assessment(symbol=symbol, timeframe=timeframe)

    def get_anomalies(
        self,
        symbol: Optional[str] = None,
        severity: Optional[QualitySeverity] = None,
        limit: int = 50,
    ) -> List[MarketAnomaly]:
        """Query bounded anomaly history from monitor."""
        return self.monitor.get_anomalies(symbol=symbol, severity=severity, limit=limit)

    def get_quality_summary(self) -> Dict[str, Any]:
        """Return operational quality summary."""
        return self.monitor.get_quality_summary()

    def reset(self) -> None:
        """Reset internal caches and tracking state."""
        with self._lock:
            self._seen_fingerprints.clear()
            self._latest_stream_timestamps.clear()
            self.source_tracker = SourceHealthTracker()
            self.monitor.reset()

    def reset_stream(
        self,
        symbol: str,
        source: Optional[str] = None,
        data_type: Optional[str] = "bar",
    ) -> None:
        """Reset timestamp and fingerprint tracking for a specific symbol/stream."""
        with self._lock:
            norm_sym = symbol.strip().upper()
            keys_to_del = [
                k for k in self._latest_stream_timestamps
                if k[1].upper() == norm_sym
                and (source is None or k[0] == source)
                and (data_type is None or k[2] == data_type)
            ]
            for k in keys_to_del:
                self._latest_stream_timestamps.pop(k, None)

            # Evict fingerprints matching this stream
            fps_to_del = [
                fp for fp in self._seen_fingerprints
                if f":{norm_sym}:" in fp
                and (source is None or fp.startswith(f"{source}:"))
            ]
            for fp in fps_to_del:
                self._seen_fingerprints.pop(fp, None)
