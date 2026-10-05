"""KAISER — Deterministic Historical Market Data Replay Engine.

Executes controlled, reproducible playback of stored historical OHLCV bars
through the MarketDataQualityEngine and TechnicalAnalysisEngine.

Key Architectural Guarantees:
1. Replays stored bars in strictly chronological order.
2. Isolates and resets analysis state between runs to avoid state contamination.
3. Configurable pacing: zero-latency immediate playback for fast testing,
   or positive pacing for real-time emulation.
4. Full quality gating: corrupt or invalid bars are quarantined and never enter
   the rolling calculation window.
5. Observable lifecycle: emits STARTED, PROGRESS, COMPLETED, CANCELLED, FAILED events.
6. Clearly labeled replay data (source='replay'), separated from live market prices.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from app.analysis.engine import TechnicalAnalysisEngine
from app.analysis.models import TIMEFRAME_SECONDS, normalize_timeframe
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.historical.storage import HistoricalBarRepository
from app.market.models import Bar
from app.market.quality.engine import MarketDataQualityEngine
from app.market.quality.status import DataQualityStatus

logger = get_logger("market.historical.replay")


class HistoricalReplayStatus(str, Enum):
    """Execution status of a historical replay session."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class HistoricalReplayRequest:
    """Request specification for replaying historical market data.

    Attributes:
        symbol: Ticker symbol.
        timeframe: Bar timeframe ('1m', '5m', '15m', '1h', '1D').
        start_time: Start POSIX UTC timestamp.
        end_time: End POSIX UTC timestamp.
        limit: Maximum bars to replay.
        pacing_seconds: Sleep interval between bars (0.0 for immediate fast replay).
        reset_analysis_state: If True, reset stream in TechnicalAnalysisEngine before replay.
        replay_id: Optional unique replay run identifier.
        correlation_id: Tracing correlation ID.
    """

    symbol: str
    timeframe: str
    start_time: float
    end_time: float
    limit: Optional[int] = 5000
    pacing_seconds: float = 0.0
    reset_analysis_state: bool = True
    replay_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("HistoricalReplayRequest symbol must be a non-empty string.")
        object.__setattr__(self, "symbol", self.symbol.strip().upper())

        canon_tf = normalize_timeframe(self.timeframe)
        object.__setattr__(self, "timeframe", canon_tf)

        if self.start_time <= 0:
            raise ValidationError(f"HistoricalReplayRequest start_time must be positive, got {self.start_time}")
        if self.end_time <= 0:
            raise ValidationError(f"HistoricalReplayRequest end_time must be positive, got {self.end_time}")
        if self.end_time < self.start_time:
            raise ValidationError(
                f"HistoricalReplayRequest end_time ({self.end_time}) cannot be earlier than start_time ({self.start_time})"
            )

        if self.limit is not None and (not isinstance(self.limit, int) or self.limit <= 0):
            raise ValidationError("HistoricalReplayRequest limit must be a positive integer.")

        if self.pacing_seconds < 0.0:
            raise ValidationError("HistoricalReplayRequest pacing_seconds cannot be negative.")

        if not self.replay_id:
            object.__setattr__(self, "replay_id", f"replay_{uuid.uuid4().hex[:12]}")
        if not self.correlation_id:
            object.__setattr__(self, "correlation_id", uuid.uuid4().hex)

    def to_dict(self) -> Dict[str, Any]:
        """Convert request to dictionary."""
        return {
            "replay_id": self.replay_id,
            "correlation_id": self.correlation_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "limit": self.limit,
            "pacing_seconds": self.pacing_seconds,
            "reset_analysis_state": self.reset_analysis_state,
        }


@dataclass(frozen=True)
class HistoricalReplayResult:
    """Audit report and outcome metrics of a historical replay execution."""

    replay_id: str
    status: HistoricalReplayStatus
    request: HistoricalReplayRequest
    total_bars_available: int
    bars_replayed: int
    bars_accepted: int
    bars_rejected: int
    bars_duplicate: int
    zero_volume_bars: int
    gaps_detected: int
    vectors_computed: int
    start_timestamp: Optional[float] = None
    end_timestamp: Optional[float] = None
    duration_ms: float = 0.0
    error_message: Optional[str] = None
    rejection_reasons: Tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "rejection_reasons", tuple(self.rejection_reasons or ()))

    @property
    def is_success(self) -> bool:
        """True if replay completed successfully."""
        return self.status == HistoricalReplayStatus.COMPLETED

    @property
    def summary_text(self) -> str:
        """Human/voice readable replay summary."""
        return (
            f"Historical Replay [{self.replay_id}] for {self.request.symbol} ({self.request.timeframe}): "
            f"Status={self.status.value}, "
            f"Available={self.total_bars_available}, "
            f"Replayed={self.bars_replayed}, "
            f"Accepted={self.bars_accepted}, "
            f"Rejected={self.bars_rejected}, "
            f"Vectors={self.vectors_computed}, "
            f"Duration={self.duration_ms:.1f}ms"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize result to dictionary."""
        return {
            "replay_id": self.replay_id,
            "status": self.status.value,
            "symbol": self.request.symbol,
            "timeframe": self.request.timeframe,
            "total_bars_available": self.total_bars_available,
            "bars_replayed": self.bars_replayed,
            "bars_accepted": self.bars_accepted,
            "bars_rejected": self.bars_rejected,
            "bars_duplicate": self.bars_duplicate,
            "zero_volume_bars": self.zero_volume_bars,
            "gaps_detected": self.gaps_detected,
            "vectors_computed": self.vectors_computed,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "duration_ms": round(self.duration_ms, 2),
            "error_message": self.error_message,
            "rejection_reasons": list(self.rejection_reasons),
            "summary": self.summary_text,
        }


class HistoricalReplayEngine:
    """Orchestrates deterministic replay of stored historical market data."""

    def __init__(
        self,
        repository: HistoricalBarRepository,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: TechnicalAnalysisEngine,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        provenance_tracker: Optional[Any] = None,
        name: str = "historical_replay_engine",
    ) -> None:
        self.name = name
        self.repository = repository
        self.quality_engine = quality_engine
        self.analysis_engine = analysis_engine
        self.event_bus = event_bus
        self.state_store = state_store
        self.provenance_tracker = provenance_tracker

        self._lock = threading.RLock()
        self._active_replays: Dict[str, threading.Event] = {}

    @property
    def active_replay_count(self) -> int:
        """Return the number of currently active replay sessions."""
        with self._lock:
            return len(self._active_replays)

    def cancel_replay(self, replay_id: str) -> bool:
        """Signal cancellation to an active replay session."""
        with self._lock:
            event = self._active_replays.get(replay_id)
            if event:
                event.set()
                logger.info("Cancellation requested for replay '%s'", replay_id)
                return True
            return False

    def replay(
        self,
        request: HistoricalReplayRequest,
        cancellation_token: Optional[threading.Event] = None,
    ) -> HistoricalReplayResult:
        """Execute historical replay of stored bars through the analysis pipeline."""
        start_time = time.perf_counter()
        replay_id = request.replay_id or f"replay_{uuid.uuid4().hex[:12]}"
        cid = request.correlation_id or uuid.uuid4().hex

        # Setup cancellation event
        stop_event = cancellation_token or threading.Event()
        with self._lock:
            self._active_replays[replay_id] = stop_event

        # 1. State Isolation: Reset analysis stream and quality tracking if requested
        if request.reset_analysis_state:
            self.analysis_engine.reset_stream(request.symbol, request.timeframe)
            if hasattr(self.quality_engine, "reset_stream"):
                self.quality_engine.reset_stream(request.symbol, source="replay")
            elif hasattr(self.quality_engine, "reset"):
                self.quality_engine.reset()
            logger.info(
                "Reset analysis stream for %s (%s) ahead of replay [%s]",
                request.symbol,
                request.timeframe,
                replay_id,
            )

        # 2. Publish Started Event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.HISTORICAL_REPLAY_STARTED,
                    source=self.name,
                    correlation_id=cid,
                    payload=request.to_dict(),
                )
            )

        # 3. Retrieve Stored Bars
        try:
            stored_bars = self.repository.query_bars(
                symbol=request.symbol,
                timeframe=request.timeframe,
                start_time=request.start_time,
                end_time=request.end_time,
                limit=request.limit or 5000,
            )
        except Exception as err:
            logger.error("Failed to query bars for replay [%s]: %s", replay_id, err)
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            result = HistoricalReplayResult(
                replay_id=replay_id,
                status=HistoricalReplayStatus.FAILED,
                request=request,
                total_bars_available=0,
                bars_replayed=0,
                bars_accepted=0,
                bars_rejected=0,
                bars_duplicate=0,
                zero_volume_bars=0,
                gaps_detected=0,
                vectors_computed=0,
                duration_ms=duration_ms,
                error_message=str(err),
            )
            self._cleanup_replay(replay_id)
            self._publish_terminal_event(result, cid)
            return result

        total_available = len(stored_bars)
        if total_available == 0:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            result = HistoricalReplayResult(
                replay_id=replay_id,
                status=HistoricalReplayStatus.COMPLETED,
                request=request,
                total_bars_available=0,
                bars_replayed=0,
                bars_accepted=0,
                bars_rejected=0,
                bars_duplicate=0,
                zero_volume_bars=0,
                gaps_detected=0,
                vectors_computed=0,
                duration_ms=duration_ms,
            )
            self._cleanup_replay(replay_id)
            self._publish_terminal_event(result, cid)
            return result

        # 4. Chronological Playback Loop
        bars_replayed = 0
        bars_accepted = 0
        bars_rejected = 0
        bars_duplicate = 0
        zero_vol_count = 0
        gaps_detected = 0
        vectors_computed = 0
        rejection_reasons: List[str] = []

        interval_sec = TIMEFRAME_SECONDS.get(request.timeframe, 60.0)
        last_ts: Optional[float] = None
        first_ts = stored_bars[0].timestamp
        final_ts = stored_bars[-1].timestamp

        for idx, bar in enumerate(stored_bars):
            # Check cancellation
            if stop_event.is_set():
                logger.warning("Historical replay [%s] cancelled after %d bars", replay_id, bars_replayed)
                duration_ms = (time.perf_counter() - start_time) * 1000.0
                result = HistoricalReplayResult(
                    replay_id=replay_id,
                    status=HistoricalReplayStatus.CANCELLED,
                    request=request,
                    total_bars_available=total_available,
                    bars_replayed=bars_replayed,
                    bars_accepted=bars_accepted,
                    bars_rejected=bars_rejected,
                    bars_duplicate=bars_duplicate,
                    zero_volume_bars=zero_vol_count,
                    gaps_detected=gaps_detected,
                    vectors_computed=vectors_computed,
                    start_timestamp=first_ts,
                    end_timestamp=last_ts or first_ts,
                    duration_ms=duration_ms,
                    rejection_reasons=tuple(rejection_reasons),
                )
                self._cleanup_replay(replay_id)
                self._publish_terminal_event(result, cid)

                if getattr(self, "provenance_tracker", None) is not None:
                    try:
                        self.provenance_tracker.create_replay_provenance(
                            replay_id=result.replay_id,
                            symbol=result.request.symbol,
                            timeframe=result.request.timeframe,
                            bars_replayed=result.bars_replayed,
                            start_timestamp=result.start_timestamp,
                            end_timestamp=result.end_timestamp,
                            operation_id=result.replay_id,
                            metadata={"status": "CANCELLED", "cancellation_requested": True},
                        )
                    except Exception as exc:
                        logger.warning("Failed to record cancelled replay provenance: %s", exc)

                return result

            # Apply configurable replay pacing
            if request.pacing_seconds > 0.0:
                time.sleep(request.pacing_seconds)

            # Ensure stamped as replay data
            if bar.source != "replay":
                bar_to_ingest = Bar(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    timeframe=bar.timeframe,
                    timestamp=bar.timestamp,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    source="replay",
                )
            else:
                bar_to_ingest = bar

            # Check gap
            if last_ts is not None:
                diff = bar_to_ingest.timestamp - last_ts
                if diff > interval_sec * 1.5:
                    gaps_detected += 1
            last_ts = bar_to_ingest.timestamp

            if bar_to_ingest.volume == 0.0:
                zero_vol_count += 1

            bars_replayed += 1

            # Quality Gate Evaluation (with candle timestamp reference)
            assessment = self.quality_engine.evaluate_bar(
                bar=bar_to_ingest,
                correlation_id=cid,
                reference_time=bar_to_ingest.timestamp,
            )

            if not assessment.safe_to_consume:
                bars_rejected += 1
                rejection_reasons.append(
                    f"Quarantined by quality engine at ts={bar_to_ingest.timestamp}: {assessment.quality_status.value}"
                )
                continue

            # Ingest into TechnicalAnalysisEngine
            vector = self.analysis_engine.add_bar(
                bar=bar_to_ingest,
                safe_to_consume=True,
                correlation_id=cid,
            )

            if vector is not None:
                bars_accepted += 1
                vectors_computed += 1
            else:
                # Rejected as duplicate or out-of-order by series
                bars_duplicate += 1

            # Emit Progress Event periodically (every 50 bars or at 25%, 50%, 75%)
            if self.event_bus and (bars_replayed % 50 == 0 or bars_replayed == total_available):
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REPLAY_PROGRESS,
                        source=self.name,
                        correlation_id=cid,
                        payload={
                            "replay_id": replay_id,
                            "symbol": request.symbol,
                            "timeframe": request.timeframe,
                            "bars_replayed": bars_replayed,
                            "total_bars": total_available,
                            "percent_complete": round((bars_replayed / total_available) * 100.0, 1),
                        },
                    )
                )

        duration_ms = (time.perf_counter() - start_time) * 1000.0
        result = HistoricalReplayResult(
            replay_id=replay_id,
            status=HistoricalReplayStatus.COMPLETED,
            request=request,
            total_bars_available=total_available,
            bars_replayed=bars_replayed,
            bars_accepted=bars_accepted,
            bars_rejected=bars_rejected,
            bars_duplicate=bars_duplicate,
            zero_volume_bars=zero_vol_count,
            gaps_detected=gaps_detected,
            vectors_computed=vectors_computed,
            start_timestamp=first_ts,
            end_timestamp=final_ts,
            duration_ms=duration_ms,
            rejection_reasons=tuple(rejection_reasons),
        )

        self._cleanup_replay(replay_id)
        self._publish_terminal_event(result, cid)

        # Update StateStore
        if self.state_store:
            self.state_store.set("last_replay_result", result.to_dict())
            self.state_store.update("replays_completed_count", lambda c: (c or 0) + 1, default=0)

        # Record Market Data Lineage & Provenance
        if getattr(self, "provenance_tracker", None) is not None:
            try:
                self.provenance_tracker.create_replay_provenance(
                    replay_id=result.replay_id,
                    symbol=result.request.symbol,
                    timeframe=result.request.timeframe,
                    bars_replayed=result.bars_replayed,
                    start_timestamp=result.start_timestamp,
                    end_timestamp=result.end_timestamp,
                    operation_id=result.replay_id,
                )
            except Exception as exc:
                logger.warning("Failed to record replay provenance: %s", exc)

        logger.info("Historical replay [%s] completed: %s", replay_id, result.summary_text)
        return result

    def _cleanup_replay(self, replay_id: str) -> None:
        """Remove active replay registration."""
        with self._lock:
            self._active_replays.pop(replay_id, None)

    def _publish_terminal_event(self, result: HistoricalReplayResult, correlation_id: str) -> None:
        """Publish terminal replay lifecycle event."""
        if not self.event_bus:
            return

        event_map = {
            HistoricalReplayStatus.COMPLETED: EventType.HISTORICAL_REPLAY_COMPLETED,
            HistoricalReplayStatus.CANCELLED: EventType.HISTORICAL_REPLAY_CANCELLED,
            HistoricalReplayStatus.FAILED: EventType.HISTORICAL_REPLAY_FAILED,
        }
        ev_type = event_map.get(result.status, EventType.HISTORICAL_REPLAY_FAILED)
        self.event_bus.publish(
            Event.create(
                event_type=ev_type,
                source=self.name,
                correlation_id=correlation_id,
                payload=result.to_dict(),
                metadata={"is_replay": True},
            )
        )
