"""KAISER — Historical Market Data Loader & Pipeline Integration.

Coordinates the loading, normalization, quality evaluation, and pipeline ingestion
of historical OHLCV candlestick records into the KAISER Technical Analysis Engine.

Enforces:
1. Provider independence and error isolation.
2. Route historical bars through MarketDataQualityEngine before analysis ingestion.
3. Strict isolation: rejected/quarantined bars NEVER enter RollingBarSeries.
4. Deterministic gap detection without fabricating synthetic candles.
5. Preserves distinction between missing intervals and valid zero-volume bars.
6. Idempotent ingestion: duplicate historical bars are safely tracked and ignored.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

if TYPE_CHECKING:
    from app.analysis.engine import TechnicalAnalysisEngine

from app.analysis.models import SUPPORTED_TIMEFRAMES, TIMEFRAME_SECONDS, normalize_timeframe
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalIngestionResult,
    HistoricalProviderStatus,
)
from app.market.historical.provider import HistoricalDataProvider
from app.market.models import Bar
from app.market.normalizer import MarketDataNormalizer, parse_timestamp
from app.market.quality.engine import MarketDataQualityEngine

logger = get_logger("market.historical.loader")


class HistoricalDataLoader:
    """Orchestrates historical market data loading, quality gating, and analysis ingestion."""

    def __init__(
        self,
        provider: HistoricalDataProvider,
        quality_engine: MarketDataQualityEngine,
        analysis_engine: Optional[TechnicalAnalysisEngine] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        name: str = "historical_data_loader",
    ) -> None:
        self.name = name
        self.provider = provider
        self.quality_engine = quality_engine
        self.analysis_engine = analysis_engine
        self.event_bus = event_bus
        self.state_store = state_store
        self.normalizer = MarketDataNormalizer()

    def set_provider(self, provider: HistoricalDataProvider) -> None:
        """Dynamically swap or update the historical data provider."""
        self.provider = provider
        logger.info("Updated historical data provider to '%s'", provider.name)

    def load_bars(
        self,
        request: HistoricalBarRequest,
        paginate_all: bool = False,
        max_pages: int = 10,
        correlation_id: Optional[str] = None,
    ) -> HistoricalBarResponse:
        """Fetch historical bars from the provider, supporting bounded pagination.

        Args:
            request: Validated HistoricalBarRequest.
            paginate_all: If True, recursively follow next_page_token up to max_pages.
            max_pages: Safeguard against infinite pagination loops.
            correlation_id: Tracing correlation ID.

        Returns:
            HistoricalBarResponse: Consolidated provider response.
        """
        cid = correlation_id or uuid.uuid4().hex

        # Publish request event
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.HISTORICAL_DATA_REQUESTED,
                    source=self.name,
                    correlation_id=cid,
                    payload=request.to_dict(),
                )
            )

        initial_response = self.provider.get_historical_bars(request)

        if not paginate_all or not initial_response.next_page_token or initial_response.status != HistoricalProviderStatus.PARTIAL:
            self._publish_loaded_event(initial_response, cid)
            return initial_response

        # Follow pagination cursors
        accumulated_bars: List[Bar] = list(initial_response.bars)
        current_token: Optional[str] = initial_response.next_page_token
        pages_fetched = 1
        limit = request.limit or 5000

        while current_token and pages_fetched < max_pages and len(accumulated_bars) < limit:
            paged_request = HistoricalBarRequest(
                symbol=request.symbol,
                timeframe=request.timeframe,
                start_time=request.start_time,
                end_time=request.end_time,
                limit=min(limit - len(accumulated_bars), request.limit or 1000),
                page_token=current_token,
                exchange=request.exchange,
            )
            next_resp = self.provider.get_historical_bars(paged_request)
            if next_resp.status not in (HistoricalProviderStatus.SUCCESS, HistoricalProviderStatus.PARTIAL):
                logger.warning(
                    "Pagination interrupted on page %d for %s: %s",
                    pages_fetched + 1,
                    request.symbol,
                    next_resp.error_message,
                )
                break

            accumulated_bars.extend(next_resp.bars)
            current_token = next_resp.next_page_token
            pages_fetched += 1

        final_response = HistoricalBarResponse(
            request=request,
            status=HistoricalProviderStatus.SUCCESS if not current_token else HistoricalProviderStatus.PARTIAL,
            bars=tuple(accumulated_bars),
            next_page_token=current_token,
            provider_name=self.provider.name,
        )

        self._publish_loaded_event(final_response, cid)
        return final_response

    def load_and_ingest(
        self,
        request: HistoricalBarRequest,
        correlation_id: Optional[str] = None,
        paginate_all: bool = False,
        max_pages: int = 10,
    ) -> HistoricalIngestionResult:
        """Fetch historical bars and ingest validated bars into the analysis pipeline.

        Args:
            request: Validated HistoricalBarRequest.
            correlation_id: Tracing correlation ID.
            paginate_all: If True, fetch all available pages.
            max_pages: Maximum pagination depth guard.

        Returns:
            HistoricalIngestionResult: Detailed audit report of the ingestion.
        """
        start_time = time.perf_counter()
        cid = correlation_id or uuid.uuid4().hex

        # 1. Fetch from provider
        response = self.load_bars(
            request=request,
            paginate_all=paginate_all,
            max_pages=max_pages,
            correlation_id=cid,
        )

        # 2. Check for provider-level terminal failures
        if response.status in (
            HistoricalProviderStatus.PROVIDER_ERROR,
            HistoricalProviderStatus.UNSUPPORTED_REQUEST,
            HistoricalProviderStatus.RATE_LIMITED,
        ):
            err_msg = response.error_message or f"Provider returned {response.status.value}"
            logger.error("Historical data provider error for %s: %s", request.symbol, err_msg)

            if self.event_bus:
                self.event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_DATA_FAILED,
                        source=self.name,
                        correlation_id=cid,
                        payload={
                            "symbol": request.symbol,
                            "timeframe": request.timeframe,
                            "status": response.status.value,
                            "error": err_msg,
                        },
                    )
                )

            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return HistoricalIngestionResult(
                request=request,
                provider_status=response.status,
                received_bars_count=0,
                accepted_bars_count=0,
                rejected_bars_count=0,
                duplicate_bars_count=0,
                zero_volume_bars_count=0,
                detected_gaps_count=0,
                rejection_reasons=(err_msg,),
                correlation_id=cid,
                execution_time_ms=duration_ms,
            )

        # 3. Handle Empty Response
        if response.is_empty:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return HistoricalIngestionResult(
                request=request,
                provider_status=HistoricalProviderStatus.EMPTY,
                received_bars_count=0,
                accepted_bars_count=0,
                rejected_bars_count=0,
                duplicate_bars_count=0,
                zero_volume_bars_count=0,
                detected_gaps_count=0,
                rejection_reasons=(),
                correlation_id=cid,
                execution_time_ms=duration_ms,
            )

        # 4. Ingest and Quality-Gate Bars
        return self._ingest_bars(
            request=request,
            bars=response.bars,
            provider_status=response.status,
            correlation_id=cid,
            start_perf_time=start_time,
        )

    def ingest_raw_records(
        self,
        raw_records: Sequence[Dict[str, Any]],
        request: HistoricalBarRequest,
        correlation_id: Optional[str] = None,
    ) -> HistoricalIngestionResult:
        """Normalize and ingest raw dictionary records directly.

        Useful for loading external JSON dumps, CSV records, or offline fixtures.
        """
        start_time = time.perf_counter()
        cid = correlation_id or uuid.uuid4().hex

        valid_bars: List[Bar] = []
        raw_rejection_reasons: List[str] = []

        for idx, raw in enumerate(raw_records):
            try:
                bar = self.normalizer.normalize_bar(raw, default_source=f"{self.name}_raw")
                valid_bars.append(bar)
            except ValidationError as err:
                raw_rejection_reasons.append(f"Record #{idx} normalization failed: {err}")

        result = self._ingest_bars(
            request=request,
            bars=valid_bars,
            provider_status=HistoricalProviderStatus.SUCCESS,
            correlation_id=cid,
            start_perf_time=start_time,
            additional_rejections=raw_rejection_reasons,
        )
        return result

    def _ingest_bars(
        self,
        request: HistoricalBarRequest,
        bars: Sequence[Bar],
        provider_status: HistoricalProviderStatus,
        correlation_id: str,
        start_perf_time: float,
        additional_rejections: Optional[List[str]] = None,
    ) -> HistoricalIngestionResult:
        """Internal worker executing quality evaluation, gap detection, and series admission."""
        received_count = len(bars) + (len(additional_rejections) if additional_rejections else 0)
        accepted_count = 0
        rejected_count = len(additional_rejections) if additional_rejections else 0
        duplicate_count = 0
        zero_vol_count = 0
        rejection_reasons: List[str] = list(additional_rejections or [])

        # 1. Validate Symbol & Timeframe Consistency
        sanitized_bars: List[Bar] = []
        for b in bars:
            norm_b_sym = b.symbol.strip().upper()
            norm_b_tf = normalize_timeframe(b.timeframe)
            if norm_b_sym != request.symbol or norm_b_tf != request.timeframe:
                rejected_count += 1
                rejection_reasons.append(
                    f"Symbol/Timeframe mismatch for bar at {b.timestamp}: "
                    f"expected {request.symbol}/{request.timeframe}, got {b.symbol}/{b.timeframe}"
                )
            else:
                sanitized_bars.append(b)

        # 2. Chronological Ordering & Gap Detection
        # Sort by timestamp to enforce strictly ascending processing order
        ordered_bars = sorted(sanitized_bars, key=lambda x: x.timestamp)
        interval_sec = TIMEFRAME_SECONDS.get(request.timeframe, 60)

        detected_gaps: List[Dict[str, Any]] = []
        for i in range(len(ordered_bars) - 1):
            t_curr = ordered_bars[i].timestamp
            t_next = ordered_bars[i + 1].timestamp
            diff = t_next - t_curr

            if diff > interval_sec * 1.5:
                missed = max(1, int(round(diff / interval_sec)) - 1)
                detected_gaps.append({
                    "from_timestamp": t_curr,
                    "to_timestamp": t_next,
                    "gap_seconds": diff,
                    "missed_intervals": missed,
                })

        # 3. Route Through MarketDataQualityEngine & Ingest
        first_ts: Optional[float] = ordered_bars[0].timestamp if ordered_bars else None
        last_ts: Optional[float] = ordered_bars[-1].timestamp if ordered_bars else None

        for bar in ordered_bars:
            if bar.volume == 0.0:
                zero_vol_count += 1

            # Quality evaluation: Pass reference_time=bar.timestamp to evaluate data integrity
            # relative to the candle's historical occurrence, preventing false-positive wall-clock staleness.
            assessment = self.quality_engine.evaluate_bar(
                bar=bar,
                correlation_id=correlation_id,
                reference_time=bar.timestamp,
            )

            if not assessment.safe_to_consume:
                is_dup = (
                    getattr(assessment, "duplicate_status", "") == "DUPLICATE"
                    or any("Duplicate record detected" in w for w in assessment.warnings)
                )
                if is_dup:
                    duplicate_count += 1
                else:
                    rejected_count += 1
                    rejection_reasons.append(
                        f"Quarantined by quality engine at {bar.timestamp}: "
                        f"{assessment.quality_status.value} (warnings={list(assessment.warnings)}, errors={list(assessment.errors)})"
                    )
                continue

            # Ingest into TechnicalAnalysisEngine if present
            if self.analysis_engine:
                added = self.analysis_engine.add_bar(
                    bar=bar,
                    safe_to_consume=True,
                    correlation_id=correlation_id,
                )
                if added is None:
                    # Bar was not added (e.g. duplicate or out-of-order timestamp in series)
                    duplicate_count += 1
                else:
                    accepted_count += 1
            else:
                accepted_count += 1

        duration_ms = (time.perf_counter() - start_perf_time) * 1000.0

        result = HistoricalIngestionResult(
            request=request,
            provider_status=provider_status,
            received_bars_count=received_count,
            accepted_bars_count=accepted_count,
            rejected_bars_count=rejected_count,
            duplicate_bars_count=duplicate_count,
            zero_volume_bars_count=zero_vol_count,
            detected_gaps_count=len(detected_gaps),
            gap_details=tuple(detected_gaps),
            rejection_reasons=tuple(rejection_reasons),
            first_bar_timestamp=first_ts,
            last_bar_timestamp=last_ts,
            correlation_id=correlation_id,
            execution_time_ms=duration_ms,
        )

        # 4. StateStore Updates
        if self.state_store:
            self.state_store.set("last_historical_ingestion", result.to_dict())
            self.state_store.update("historical_requests_count", lambda c: (c or 0) + 1, default=0)
            self.state_store.update("historical_bars_loaded_count", lambda c: (c or 0) + received_count, default=0)
            self.state_store.update("historical_bars_accepted_count", lambda c: (c or 0) + accepted_count, default=0)

        # 5. EventBus Notification
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.HISTORICAL_DATA_INGESTED,
                    source=self.name,
                    correlation_id=correlation_id,
                    payload=result.to_dict(),
                    metadata={
                        "is_historical": True,
                        "source_provider": self.provider.name,
                    },
                )
            )

        logger.info(
            "Historical ingestion completed for %s (%s) [cid=%s]: %s",
            request.symbol,
            request.timeframe,
            correlation_id,
            result.summary_text,
        )
        return result

    def _publish_loaded_event(self, response: HistoricalBarResponse, correlation_id: str) -> None:
        """Publish HISTORICAL_DATA_LOADED notification on the EventBus."""
        if self.event_bus:
            self.event_bus.publish(
                Event.create(
                    event_type=EventType.HISTORICAL_DATA_LOADED,
                    source=self.name,
                    correlation_id=correlation_id,
                    payload=response.to_dict(),
                    metadata={
                        "is_historical": True,
                        "source_provider": response.provider_name,
                    },
                )
            )
