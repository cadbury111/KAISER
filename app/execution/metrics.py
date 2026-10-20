"""KAISER — Paper Trading Order Lifecycle Metrics & Execution Latency Profiling.

Provides strongly-typed, deterministic observability for paper trading execution:
1. Order lifecycle tracking across all supported workflow stages with deduplication
   and bounded memory storage.
2. High-precision execution latency profiling across workflow phases with deterministic
   percentile calculations (median, p95) and injectable clocks.
3. Cryptographic digest verification of lifecycle metrics and latency profiles.

SAFETY INVARIANTS:
1. PURELY OBSERVATIONAL: Metrics collection never mutates order state, portfolio cash,
   positions, or financial ledger balances.
2. ZERO LIVE EXECUTION: In-process profiling only; local simulated latency does NOT
   predict live broker, network, or exchange matching latency.
3. FAIL CLOSED: Invalid (negative, non-finite) latency measurements are safely excluded
   without raising unhandled exceptions or corrupting aggregates.
"""

from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import math
import threading
import time
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Set, Tuple
import uuid

from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger

logger = get_logger("execution.metrics")


class OrderLifecycleStage(str, Enum):
    """Supported lifecycle stages of a paper trading order workflow."""

    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    REQUEST_VALIDATED = "REQUEST_VALIDATED"
    QUOTE_VALIDATED = "QUOTE_VALIDATED"
    RISK_CHECKED = "RISK_CHECKED"
    ORDER_ACCEPTED = "ORDER_ACCEPTED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_PENDING = "ORDER_PENDING"
    FILL_GENERATED = "FILL_GENERATED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    LEDGER_PERSISTED = "LEDGER_PERSISTED"
    LEDGER_FAILED = "LEDGER_FAILED"
    PORTFOLIO_RECONCILED = "PORTFOLIO_RECONCILED"
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"
    WORKFLOW_COMPLETED = "WORKFLOW_COMPLETED"
    WORKFLOW_FAILED = "WORKFLOW_FAILED"


class LatencyStage(str, Enum):
    """Discrete operational phases measured during paper order execution."""

    REQUEST_VALIDATION = "request_validation"
    QUOTE_VALIDATION = "quote_validation"
    RISK_VALIDATION = "risk_validation"
    BROKER_PROCESSING = "broker_processing"
    LEDGER_PERSISTENCE = "ledger_persistence"
    WORKFLOW_E2E = "workflow_e2e"


@dataclass(frozen=True)
class OrderLifecycleEvent:
    """Immutable record of an order lifecycle transition."""

    event_id: str
    stage: OrderLifecycleStage
    symbol: str
    status: str
    timestamp: float
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    elapsed_ns: Optional[int] = None
    rejection_code: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.event_id:
            raise ValidationError("OrderLifecycleEvent event_id is required.")
        if not self.symbol:
            raise ValidationError("OrderLifecycleEvent symbol is required.")
        if not math.isfinite(self.timestamp) or self.timestamp <= 0:
            raise ValidationError(f"OrderLifecycleEvent timestamp must be a positive finite float, got {self.timestamp}")

    @property
    def replay_session_id(self) -> Optional[str]:
        """Alias for session_id."""
        return self.session_id

    def to_dict(self) -> Dict[str, Any]:
        """Convert lifecycle event to serializable dictionary."""
        return {
            "event_id": self.event_id,
            "stage": self.stage.value,
            "status": self.status,
            "symbol": self.symbol,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "correlation_id": self.correlation_id,
            "session_id": self.session_id,
            "replay_session_id": self.session_id,
            "timestamp": self.timestamp,
            "elapsed_ns": self.elapsed_ns,
            "rejection_code": self.rejection_code,
            "details": dict(self.details),
        }


def calculate_quantiles(
    samples: Sequence[float],
    quantile: Optional[float] = None,
) -> Union[Optional[float], Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]]:
    """Compute min, max, mean, median, and p95 for a sequence of numeric samples.

    Percentile Method:
        Standard linear interpolation between sorted values:
        For index q * (N - 1):
            lo = floor(idx), hi = ceil(idx), fraction = idx - lo
            value = samples[lo] + (samples[hi] - samples[lo]) * fraction

    Boundary Handling:
        - N == 0: returns None (or tuple of None)
        - N == 1: returns s[0] (or tuple of s[0])
        - N == 2: median = (s[0] + s[1]) / 2, p95 interpolates between s[0] and s[1]
    """
    valid = [float(x) for x in samples if math.isfinite(x) and x >= 0.0]
    n = len(valid)
    if n == 0:
        if quantile is not None:
            return None
        return None, None, None, None, None

    sorted_vals = sorted(valid)

    def _percentile(p: float) -> float:
        if n == 1:
            return sorted_vals[0]
        idx = p * (n - 1)
        lo = int(math.floor(idx))
        hi = int(math.ceil(idx))
        if lo == hi:
            return sorted_vals[lo]
        fraction = idx - lo
        return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * fraction

    if quantile is not None:
        return round(_percentile(quantile), 6)

    min_val = round(sorted_vals[0], 6)
    max_val = round(sorted_vals[-1], 6)
    mean_val = round(sum(sorted_vals) / n, 6)
    median_val = round(_percentile(0.50), 6)
    p95_val = round(_percentile(0.95), 6)
    return min_val, max_val, mean_val, median_val, p95_val


@dataclass(frozen=True)
class LatencyAggregate:
    """Statistical summary of execution latencies for a specific workflow stage."""

    stage: str
    sample_count: int
    invalid_samples_count: int
    min_ms: Optional[float]
    max_ms: Optional[float]
    mean_ms: Optional[float]
    median_ms: Optional[float]
    p95_ms: Optional[float]
    unit: str = "milliseconds"

    def to_dict(self) -> Dict[str, Any]:
        """Convert aggregate to dictionary representation."""
        return {
            "stage": self.stage,
            "sample_count": self.sample_count,
            "invalid_samples_count": self.invalid_samples_count,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "mean_ms": self.mean_ms,
            "median_ms": self.median_ms,
            "p95_ms": self.p95_ms,
            "unit": self.unit,
        }


class ExecutionLatencyProfiler:
    """Low-overhead latency profiler measuring execution durations across workflow phases.

    Thread-safe, deterministic with injectable clocks, and excludes non-finite / negative durations.
    """

    def __init__(
        self,
        clock_fn: Optional[Callable[[], float]] = None,
        max_samples_per_stage: int = 5000,
    ) -> None:
        self._clock_fn = clock_fn or time.perf_counter
        self._max_samples = max_samples_per_stage
        self._lock = threading.RLock()
        self._samples: Dict[LatencyStage, deque[float]] = {
            stage: deque(maxlen=max_samples_per_stage) for stage in LatencyStage
        }
        self._invalid_counts: Dict[LatencyStage, int] = {stage: 0 for stage in LatencyStage}
        self._cumulative_counts: Dict[LatencyStage, int] = {stage: 0 for stage in LatencyStage}

    def now(self) -> float:
        """Read the current timestamp from the configured clock."""
        return self._clock_fn()

    def start_stage(self, stage: LatencyStage | str) -> float:
        """Record the start timestamp for a latency stage."""
        return self.now()

    def stop_stage(
        self,
        stage: LatencyStage | str,
        start_time: Optional[float],
    ) -> Optional[float]:
        """Record elapsed duration from start_time and return elapsed milliseconds."""
        if start_time is None:
            return None
        elapsed_sec = self.now() - start_time
        self.record_duration(stage, elapsed_sec)
        return elapsed_sec * 1000.0

    @contextmanager
    def measure_stage(self, stage: LatencyStage | str) -> Generator[None, None, None]:
        """Context manager measuring execution latency of a code block."""
        t0 = self.now()
        try:
            yield
        finally:
            elapsed_sec = self.now() - t0
            self.record_duration(stage, elapsed_sec)

    def record_stage_duration(
        self,
        stage: LatencyStage | str,
        duration_ms: float,
    ) -> bool:
        """Record an observed phase duration in milliseconds."""
        if not math.isfinite(duration_ms) or duration_ms < 0.0:
            target_stage = LatencyStage(stage) if isinstance(stage, (LatencyStage, str)) else None
            if target_stage:
                with self._lock:
                    self._invalid_counts[target_stage] += 1
            return False
        return self.record_duration(stage, duration_ms / 1000.0)

    def record_duration(
        self,
        stage: LatencyStage | str,
        duration_seconds: float,
    ) -> bool:
        """Record an observed phase duration in seconds.

        Rejects negative or non-finite values safely without raising.
        """
        try:
            target_stage = LatencyStage(stage)
        except ValueError:
            return False

        with self._lock:
            if not math.isfinite(duration_seconds) or duration_seconds < 0.0:
                self._invalid_counts[target_stage] += 1
                return False

            self._samples[target_stage].append(duration_seconds)
            self._cumulative_counts[target_stage] += 1
            return True

    def get_stage_aggregate(self, stage: LatencyStage | str) -> LatencyAggregate:
        """Compute summary statistics for a given stage in milliseconds."""
        target_stage = LatencyStage(stage) if isinstance(stage, str) else stage
        with self._lock:
            raw_samples = list(self._samples[target_stage])
            invalid_cnt = self._invalid_counts[target_stage]
            total_cnt = self._cumulative_counts[target_stage]

            # Convert to milliseconds for standard reporting
            ms_samples = [s * 1000.0 for s in raw_samples]
            min_v, max_v, mean_v, med_v, p95_v = calculate_quantiles(ms_samples)

            return LatencyAggregate(
                stage=target_stage.value,
                sample_count=len(raw_samples),
                invalid_samples_count=invalid_cnt,
                min_ms=min_v,
                max_ms=max_v,
                mean_ms=mean_v,
                median_ms=med_v,
                p95_ms=p95_v,
                unit="milliseconds",
            )

    def get_profile(self) -> Dict[str, Any]:
        """Generate a complete latency profile across all supported stages."""
        with self._lock:
            stages_data = {
                stage.value: self.get_stage_aggregate(stage).to_dict()
                for stage in LatencyStage
            }
            return {
                "unit": "milliseconds",
                "stages": stages_data,
                "disclaimer": (
                    "Simulated local in-process profiling only; does NOT reflect or "
                    "predict live exchange matching or network latency."
                ),
            }

    def get_profile_summary(self) -> Dict[str, Any]:
        """Generate profile summary enriched with fingerprint."""
        profile = self.get_profile()
        profile["fingerprint"] = self.compute_digest()
        return profile

    def compute_digest(self) -> str:
        """Compute a deterministic SHA-256 digest of the latency profile.

        Excludes non-deterministic wall-clock metadata; strictly summarizes sample counts,
        stage percentiles, and invalid observation tallies.
        """
        profile = self.get_profile()
        hasher = hashlib.sha256()
        for stage_name in sorted(profile["stages"].keys()):
            st = profile["stages"][stage_name]
            line = (
                f"STAGE:{st['stage']}:cnt={st['sample_count']}:inv={st['invalid_samples_count']}:"
                f"min={st['min_ms']}:med={st['median_ms']}:p95={st['p95_ms']}:max={st['max_ms']}\n"
            )
            hasher.update(line.encode("utf-8"))
        return hasher.hexdigest()

    def reset(self) -> None:
        """Clear all recorded latency samples and counters."""
        with self._lock:
            for stage in LatencyStage:
                self._samples[stage].clear()
                self._invalid_counts[stage] = 0
                self._cumulative_counts[stage] = 0


class OrderLifecycleTracker:
    """Thread-safe, bounded tracker recording order lifecycle transitions and metrics.

    Features:
    - Bounded event retention to prevent unbounded memory growth during long runs.
    - Cumulative streaming counters for all supported stages and terminal states.
    - Idempotent deduplication so re-emitted identical stages do not inflate counts.
    - Deterministic reporting and cryptographic digest fingerprinting.
    """

    def __init__(
        self,
        max_retained_events: int = 1000,
        profiler: Optional[ExecutionLatencyProfiler] = None,
        clock_fn: Optional[Callable[[], float]] = None,
        max_events: Optional[int] = None,
    ) -> None:
        capacity = max_events if max_events is not None else max_retained_events
        self._max_retained = capacity
        self.profiler = profiler or ExecutionLatencyProfiler(clock_fn=clock_fn)
        self._clock_fn = clock_fn or time.time
        self._lock = threading.RLock()

        # Bounded event history
        self._events: deque[OrderLifecycleEvent] = deque(maxlen=capacity)

        # Deduplication tracking: (identity_key, stage)
        self._seen_stage_transitions: Set[Tuple[str, OrderLifecycleStage]] = set()
        self._duplicate_events_count: int = 0

        # Cumulative counters
        self._stage_counts: Dict[OrderLifecycleStage, int] = {
            stage: 0 for stage in OrderLifecycleStage
        }
        self._status_counts: Dict[str, int] = {
            "ACCEPTED": 0,
            "FILLED": 0,
            "PARTIALLY_FILLED": 0,
            "PENDING": 0,
            "REJECTED": 0,
            "CANCELLED": 0,
            "FAILED": 0,
        }
        self._distinct_client_order_ids: Set[str] = set()
        self._distinct_order_ids: Set[str] = set()
        self._order_alias_map: Dict[str, str] = {}
        self._terminal_order_ids: Set[str] = set()

    def record_stage(
        self,
        stage: OrderLifecycleStage,
        symbol: str = "UNKNOWN",
        status: str = "PENDING",
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        session_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        elapsed_ns: Optional[int] = None,
        rejection_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> OrderLifecycleEvent:
        """Record an order lifecycle transition with deduplication.

        Returns:
            OrderLifecycleEvent: Created (or deduplicated) event record.
        """
        now = timestamp if timestamp is not None else self._clock_fn()
        clean_sym = symbol.strip().upper() if symbol else "UNKNOWN"
        primary_id = client_order_id or order_id or correlation_id or "anonymous"
        dedup_key = (primary_id, stage)

        with self._lock:
            # Map aliases if both client_order_id and order_id are provided
            if client_order_id and order_id:
                self._order_alias_map[order_id] = client_order_id
            canonical_id = self._order_alias_map.get(primary_id, primary_id)

            # Check for duplicate event
            if dedup_key in self._seen_stage_transitions:
                self._duplicate_events_count += 1
                for ev in reversed(self._events):
                    if (ev.client_order_id == client_order_id or ev.order_id == order_id) and ev.stage == stage:
                        return ev

            self._seen_stage_transitions.add(dedup_key)
            self._stage_counts[stage] += 1

            # Update status counts if mapped
            norm_status = status.upper().strip()
            if norm_status in self._status_counts:
                self._status_counts[norm_status] += 1

            if client_order_id:
                self._distinct_client_order_ids.add(client_order_id)
            if order_id:
                self._distinct_order_ids.add(order_id)

            # Track terminal state
            if (
                norm_status in ("FILLED", "REJECTED", "CANCELLED", "FAILED")
                or stage in (
                    OrderLifecycleStage.WORKFLOW_COMPLETED,
                    OrderLifecycleStage.FILL_GENERATED,
                    OrderLifecycleStage.ORDER_REJECTED,
                    OrderLifecycleStage.ORDER_CANCELLED,
                    OrderLifecycleStage.WORKFLOW_FAILED,
                )
            ):
                self._terminal_order_ids.add(canonical_id)

            evt = OrderLifecycleEvent(
                event_id=f"evt_{stage.value.lower()}_{uuid.uuid4().hex[:8]}",
                stage=stage,
                symbol=clean_sym,
                status=norm_status,
                timestamp=now,
                order_id=order_id,
                client_order_id=client_order_id,
                correlation_id=correlation_id,
                session_id=session_id,
                elapsed_ns=elapsed_ns,
                rejection_code=rejection_code,
                details=dict(details or {}),
            )
            self._events.append(evt)
            return evt

    def record_event(
        self,
        stage: OrderLifecycleStage,
        symbol: str = "UNKNOWN",
        status: str = "PENDING",
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        replay_session_id: Optional[str] = None,
        session_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        elapsed_ns: Optional[int] = None,
        rejection_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        message: Optional[str] = None,
    ) -> bool:
        """Convenience wrapper recording a lifecycle event. Returns True if recorded, False if duplicate."""
        primary_id = client_order_id or order_id or correlation_id or "anonymous"
        dedup_key = (primary_id, stage)
        with self._lock:
            is_dup = dedup_key in self._seen_stage_transitions

        merged_details = dict(details or {})
        if message:
            merged_details["message"] = message

        effective_session = replay_session_id or session_id

        # Determine implied status if not explicitly overridden
        effective_status = status
        if status == "PENDING":
            if stage in (OrderLifecycleStage.ORDER_ACCEPTED,):
                effective_status = "ACCEPTED"
            elif stage in (OrderLifecycleStage.FILL_GENERATED,):
                effective_status = "FILLED"
            elif stage in (OrderLifecycleStage.ORDER_REJECTED,):
                effective_status = "REJECTED"
            elif stage in (OrderLifecycleStage.ORDER_CANCELLED,):
                effective_status = "CANCELLED"
            elif stage in (OrderLifecycleStage.WORKFLOW_FAILED,):
                effective_status = "FAILED"

        self.record_stage(
            stage=stage,
            symbol=symbol,
            status=effective_status,
            order_id=order_id,
            client_order_id=client_order_id,
            correlation_id=correlation_id,
            session_id=effective_session,
            timestamp=timestamp,
            elapsed_ns=elapsed_ns,
            rejection_code=rejection_code,
            details=merged_details,
        )
        return not is_dup

    def get_events(
        self,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        stage: Optional[OrderLifecycleStage] = None,
    ) -> List[OrderLifecycleEvent]:
        """Query retained lifecycle events filtered by order_id, client_order_id, or stage."""
        with self._lock:
            events = list(self._events)
            if order_id is not None:
                events = [e for e in events if e.order_id == order_id]
            if client_order_id is not None:
                events = [e for e in events if e.client_order_id == client_order_id]
            if stage is not None:
                events = [e for e in events if e.stage == stage]
            return events

    def get_summary(self) -> Dict[str, Any]:
        """Generate a complete, strictly read-only lifecycle metrics summary."""
        with self._lock:
            stage_tallies = {stage.value.lower(): count for stage, count in self._stage_counts.items()}
            status_tallies = dict(self._status_counts)
            recent_events = [ev.to_dict() for ev in list(self._events)[-20:]]

            all_raw_ids = set(self._distinct_client_order_ids) | set(self._distinct_order_ids)
            canonical_all = {self._order_alias_map.get(oid, oid) for oid in all_raw_ids}
            if not canonical_all and self._seen_stage_transitions:
                canonical_all = {self._order_alias_map.get(k[0], k[0]) for k in self._seen_stage_transitions}

            distinct_all = len(canonical_all)
            terminal = {
                "completed": self._stage_counts[OrderLifecycleStage.WORKFLOW_COMPLETED],
                "rejected": self._status_counts["REJECTED"] or self._stage_counts[OrderLifecycleStage.ORDER_REJECTED],
                "cancelled": self._status_counts["CANCELLED"] or self._stage_counts[OrderLifecycleStage.ORDER_CANCELLED],
                "failed": self._status_counts["FAILED"] or self._stage_counts[OrderLifecycleStage.WORKFLOW_FAILED],
            }
            # Active orders are distinct orders that have not reached a terminal state
            active = max(0, len(canonical_all - self._terminal_order_ids))

            return {
                "total_events_recorded": sum(self._stage_counts.values()),
                "distinct_client_orders": len(self._distinct_client_order_ids),
                "distinct_order_ids": len(self._distinct_order_ids),
                "distinct_orders_tracked": distinct_all,
                "active_orders": active,
                "terminal_orders": terminal,
                "stage_counts": stage_tallies,
                "status_counts": status_tallies,
                "duplicate_events_ignored": self._duplicate_events_count,
                "recent_events": recent_events,
                "retained_events_capacity": self._max_retained,
                "retained_events_count": len(self._events),
                "deterministic_digest": self.compute_digest(),
            }

    def compute_digest(self) -> str:
        """Compute a deterministic SHA-256 fingerprint over lifecycle metrics."""
        with self._lock:
            hasher = hashlib.sha256()
            # Canonical stage counts
            for st_name in sorted(self._stage_counts.keys(), key=lambda s: s.value):
                hasher.update(f"STAGE:{st_name.value}:{self._stage_counts[st_name]}\n".encode("utf-8"))
            # Canonical status counts
            for stat_name in sorted(self._status_counts.keys()):
                hasher.update(f"STATUS:{stat_name}:{self._status_counts[stat_name]}\n".encode("utf-8"))
            hasher.update(f"DISTINCT_CLIENT:{len(self._distinct_client_order_ids)}\n".encode("utf-8"))
            hasher.update(f"DISTINCT_ORDERS:{len(self._distinct_order_ids)}\n".encode("utf-8"))
            return hasher.hexdigest()

    def reset(self) -> None:
        """Reset all lifecycle events, counts, and profiler metrics."""
        with self._lock:
            self._events.clear()
            self._seen_stage_transitions.clear()
            self._order_alias_map.clear()
            self._terminal_order_ids.clear()
            self._duplicate_events_count = 0
            for stage in OrderLifecycleStage:
                self._stage_counts[stage] = 0
            for st in self._status_counts:
                self._status_counts[st] = 0
            self._distinct_client_order_ids.clear()
            self._distinct_order_ids.clear()
            self.profiler.reset()
