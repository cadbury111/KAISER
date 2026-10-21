"""KAISER — Transactional Outbox Event Dispatcher & Delivery Worker.

Provides a controlled, thread-safe background delivery worker that reads pending
and retryable events from the TransactionalOutbox and publishes them to the EventBus.

DELIVERY SEMANTICS:
- At-least-once delivery: guarantees events are published upon commit; subscribers
  employ consumer deduplication (DeduplicatingEventHandler) to handle redelivery windows.
- Bounded batch processing: dispatches in configurable chunks (default 50).
- Bounded exponential backoff: transient failures retry with progressive delay up to max_backoff.
- Terminal exhaustion: events exceeding max_attempts are marked EXHAUSTED for manual/audit review.
- Deterministic testability: background thread does NOT run by default; tests drive dispatch
  directly via `dispatch_pending()` or `dispatch_all()`.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Set

from app.core.events.bus import EventBus
from app.core.exceptions import ValidationError
from app.ledger.outbox import OutboxPruneResult, OutboxRecord, OutboxStatus, TransactionalOutbox

logger = logging.getLogger("execution.outbox_dispatcher")


class OutboxDispatcher:
    """Controlled event dispatcher for publishing outbox events to the EventBus."""

    def __init__(
        self,
        outbox: TransactionalOutbox,
        event_bus: Optional[EventBus] = None,
        batch_size: int = 50,
        max_attempts: int = 5,
        retry_backoff_seconds: float = 2.0,
        max_backoff_seconds: float = 60.0,
        clock: Callable[[], float] = time.time,
        auto_start: bool = False,
        poll_interval_seconds: float = 0.5,
        retention_enabled: bool = False,
        retention_days: float = 7.0,
        prune_batch_size: int = 100,
        archival_enabled: bool = False,
    ) -> None:
        if batch_size <= 0:
            raise ValidationError(f"batch_size must be positive, got {batch_size}")
        if max_attempts <= 0:
            raise ValidationError(f"max_attempts must be positive, got {max_attempts}")
        if retry_backoff_seconds < 0:
            raise ValidationError(f"retry_backoff_seconds cannot be negative, got {retry_backoff_seconds}")
        if max_backoff_seconds < retry_backoff_seconds:
            raise ValidationError(
                f"max_backoff_seconds ({max_backoff_seconds}) must be >= retry_backoff_seconds ({retry_backoff_seconds})"
            )
        if retention_days <= 0:
            raise ValidationError(f"retention_days must be positive, got {retention_days}")
        if prune_batch_size <= 0:
            raise ValidationError(f"prune_batch_size must be positive, got {prune_batch_size}")

        self.outbox = outbox
        self.event_bus = event_bus
        self.batch_size = batch_size
        self.max_attempts = max_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.clock = clock
        self.poll_interval_seconds = max(0.05, poll_interval_seconds)
        self.retention_enabled = retention_enabled
        self.retention_days = retention_days
        self.prune_batch_size = prune_batch_size
        self.archival_enabled = archival_enabled

        self._lock = threading.RLock()
        self._dispatch_lock = threading.Lock()  # Serializes batch dispatch executions
        self._in_flight_event_ids: Set[str] = set()
        self._worker_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._is_running: bool = False
        self._last_dispatch_time: Optional[float] = None
        self._total_dispatched: int = 0
        self._total_failed_attempts: int = 0
        self._total_exhausted: int = 0

        if auto_start:
            self.start()

    @property
    def is_running(self) -> bool:
        """Check whether the background dispatch worker thread is active."""
        with self._lock:
            return self._is_running

    @property
    def in_flight_event_ids(self) -> Set[str]:
        """Get copy of currently in-flight event IDs."""
        with self._lock:
            return set(self._in_flight_event_ids)

    def start(self) -> None:
        """Start the background dispatch worker loop."""
        with self._lock:
            if self._is_running:
                logger.warning("OutboxDispatcher is already running.")
                return

            self._stop_event.clear()
            self._is_running = True
            self._worker_thread = threading.Thread(
                target=self._worker_loop,
                name="KAISER-OutboxDispatcher",
                daemon=True,
            )
            self._worker_thread.start()
            logger.info("OutboxDispatcher worker thread started.")

    def stop(self, timeout: float = 5.0) -> None:
        """Gracefully stop the background worker thread."""
        with self._lock:
            if not self._is_running:
                return

            self._is_running = False
            self._stop_event.set()

        thread = self._worker_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
            if thread.is_alive():
                logger.warning("OutboxDispatcher thread failed to stop within %.1fs timeout.", timeout)

        with self._lock:
            self._worker_thread = None
            logger.info("OutboxDispatcher stopped cleanly.")

    def _worker_loop(self) -> None:
        """Internal background loop polling and dispatching eligible outbox events."""
        while not self._stop_event.is_set():
            try:
                self.dispatch_pending(self.batch_size)
            except Exception as exc:
                logger.error("Unexpected error in OutboxDispatcher loop: %s", exc, exc_info=True)

            self._stop_event.wait(timeout=self.poll_interval_seconds)

    def dispatch_pending(self, batch_size: Optional[int] = None, now: Optional[float] = None) -> int:
        """Process a single bounded batch of eligible pending or retryable outbox events.

        Thread-safe and deterministic. Multiple concurrent calls will not process
        the same event simultaneously.

        Args:
            batch_size: Max records to process in this pass.
            now: Optional override for current timestamp (useful for testing retry schedules deterministically).

        Returns:
            int: Number of events successfully published and marked delivered in this pass.
        """
        limit = batch_size or self.batch_size
        current_time = now if now is not None else self.clock()

        with self._dispatch_lock:
            candidates = self.outbox.get_pending_batch(limit=limit * 2, now=current_time)
            eligible: List[OutboxRecord] = []

            with self._lock:
                for c in candidates:
                    if c.event_id not in self._in_flight_event_ids:
                        self._in_flight_event_ids.add(c.event_id)
                        eligible.append(c)
                        if len(eligible) >= limit:
                            break

            if not eligible:
                return 0

            successful_deliveries = 0
            try:
                for record in eligible:
                    delivered = self._dispatch_single_record(record, current_time)
                    if delivered:
                        successful_deliveries += 1
            finally:
                with self._lock:
                    for record in eligible:
                        self._in_flight_event_ids.discard(record.event_id)
                    self._last_dispatch_time = current_time

            return successful_deliveries

    def _dispatch_single_record(self, record: OutboxRecord, now: float) -> bool:
        """Attempt publication of a single outbox record to EventBus and update its status."""
        if self.event_bus is None:
            err_msg = "EventBus not configured on OutboxDispatcher"
            self._handle_failure(record, err_msg, now)
            return False

        try:
            event = record.to_event()
            self.event_bus.publish(event)

            # Mark delivered durably
            self.outbox.mark_delivered(record.event_id, delivered_at=now)
            with self._lock:
                self._total_dispatched += 1
            logger.debug(
                "Successfully dispatched outbox event [id=%s, type=%s, attempt=%d]",
                record.event_id,
                record.event_type,
                record.attempt_count + 1,
            )
            return True

        except Exception as exc:
            err_msg = f"{type(exc).__name__}: {str(exc)}"
            self._handle_failure(record, err_msg, now)
            return False

    def _handle_failure(self, record: OutboxRecord, error: str, now: float) -> None:
        """Handle publication failure, calculating backoff and setting next retry or exhaustion."""
        attempt = record.attempt_count + 1
        is_exhausted = attempt >= record.max_attempts

        with self._lock:
            self._total_failed_attempts += 1
            if is_exhausted:
                self._total_exhausted += 1

        if is_exhausted:
            self.outbox.record_failure(
                record.event_id,
                error=error,
                next_retry_at=None,
                exhausted=True,
            )
            logger.warning(
                "Outbox event exhausted retry limit (%d/%d) [id=%s, type=%s, error=%s]",
                attempt,
                record.max_attempts,
                record.event_id,
                record.event_type,
                error,
            )
        else:
            # Bounded exponential backoff: base * 2^(attempt - 1), capped at max_backoff
            backoff = min(
                self.retry_backoff_seconds * (2 ** (attempt - 1)),
                self.max_backoff_seconds,
            )
            next_retry = now + backoff
            self.outbox.record_failure(
                record.event_id,
                error=error,
                next_retry_at=next_retry,
                exhausted=False,
            )
            logger.info(
                "Outbox event delivery failed, scheduled retry #%d in %.2fs [id=%s, error=%s]",
                attempt + 1,
                backoff,
                record.event_id,
                error,
            )

    def dispatch_all(self, max_iterations: int = 100, now: Optional[float] = None) -> int:
        """Drain and dispatch all currently eligible pending records.

        Iterates until no more eligible records remain or max_iterations is reached.
        Returns:
            int: Total events delivered across all iterations.
        """
        total_delivered = 0
        iterations = 0
        while iterations < max_iterations:
            delivered = self.dispatch_pending(self.batch_size, now=now)
            total_delivered += delivered
            if delivered == 0:
                break
            iterations += 1
        return total_delivered

    def prune_delivered(
        self,
        retention_seconds: Optional[float] = None,
        batch_size: Optional[int] = None,
        archive: Optional[bool] = None,
        now: Optional[float] = None,
    ) -> OutboxPruneResult:
        """Prune older delivered records, protecting currently in-flight event IDs from deletion."""
        sec = retention_seconds if retention_seconds is not None else (self.retention_days * 86400.0)
        b_size = batch_size or self.prune_batch_size
        do_archive = archive if archive is not None else self.archival_enabled
        current_time = now if now is not None else self.clock()

        with self._lock:
            protected_ids = set(self._in_flight_event_ids)

        return self.outbox.prune_delivered_records(
            retention_seconds=sec,
            batch_size=b_size,
            archive=do_archive,
            now=current_time,
            exclude_event_ids=protected_ids,
        )

    def get_status(self) -> Dict[str, Any]:
        """Diagnostic snapshot of dispatcher lifecycle, statistics, and outbox state."""
        with self._lock:
            outbox_stats = self.outbox.get_stats()
            retention_stats = self.outbox.get_retention_status()
            return {
                "is_running": self._is_running,
                "batch_size": self.batch_size,
                "max_attempts": self.max_attempts,
                "retry_backoff_seconds": self.retry_backoff_seconds,
                "max_backoff_seconds": self.max_backoff_seconds,
                "last_dispatch_time": self._last_dispatch_time,
                "total_dispatched": self._total_dispatched,
                "total_failed_attempts": self._total_failed_attempts,
                "total_exhausted": self._total_exhausted,
                "retention_enabled": self.retention_enabled,
                "retention_days": self.retention_days,
                "prune_batch_size": self.prune_batch_size,
                "archival_enabled": self.archival_enabled,
                "outbox": outbox_stats,
                "retention": retention_stats,
            }
