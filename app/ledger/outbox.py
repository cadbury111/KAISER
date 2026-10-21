"""KAISER — Durable Transactional Outbox for Paper Trading Events.

Provides a robust, thread-safe, and SQLite-backed Transactional Outbox for
paper-trading and portfolio-allocation events.

TRANSACTION BOUNDARY GUARANTEES:
1. AT-LEAST-ONCE DELIVERY: Outbox delivery across an arbitrary decoupled EventBus
   provides at-least-once semantics. Consumer idempotency handles duplicate deliveries
   during crash windows.
2. ATOMIC LEDGER-OUTBOX COMMIT: When SQLite persistence is configured, financial ledger
   mutations and outbox events are written within the exact same database transaction.
   If ledger persistence fails, the outbox record is rolled back.
   If outbox staging fails, the financial ledger record is rolled back.
3. POST-COMMIT ISOLATION: Event delivery occurs strictly after the financial transaction
   has durably committed. Failure to deliver an event to the EventBus NEVER rolls back
   or invalidates a committed financial transaction.
4. RESTART RECOVERY: Pending and retryable events are durably recovered on process restart
   without duplicating financial entries or fills.
5. SENSITIVE DATA REDACTION: Secret tokens, credentials, and raw audio bytes are strictly
   redacted from persisted outbox payloads and diagnostic APIs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import copy
import json
import logging
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple

from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError

logger = logging.getLogger("ledger.outbox")

SAFE_EVENT_ID_REGEX = re.compile(r"^[A-Za-z0-9_\-\.]{2,128}$")

# Keys whose values must be redacted to prevent credential and audio leakage
SENSITIVE_KEY_PATTERNS = frozenset({
    "api_key",
    "secret",
    "token",
    "password",
    "auth",
    "authorization",
    "bearer",
    "private_key",
    "access_token",
    "refresh_token",
    "twelve_data_api_key",
    "dashboard_auth_token",
    "raw_audio",
    "audio_bytes",
    "audio_data",
    "voice_audio",
})


def redact_sensitive_payload(val: Any) -> Any:
    """Recursively redact sensitive credentials and bulk audio data from payload dictionaries."""
    if isinstance(val, (bytes, bytearray)):
        return "[BINARY DATA REDACTED]"
    elif isinstance(val, dict):
        clean_dict: Dict[str, Any] = {}
        for k, v in val.items():
            if isinstance(v, (bytes, bytearray)):
                clean_dict[k] = "[BINARY DATA REDACTED]"
                continue
            k_lower = str(k).lower().strip()
            if any(pattern in k_lower for pattern in SENSITIVE_KEY_PATTERNS):
                clean_dict[k] = "[REDACTED]"
            else:
                clean_dict[k] = redact_sensitive_payload(v)
        return clean_dict
    elif isinstance(val, (list, tuple)):
        return [redact_sensitive_payload(item) for item in val]
    return val


class OutboxStatus(str, Enum):
    """Lifecycle status of a transactional outbox event."""

    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
    EXHAUSTED = "EXHAUSTED"


@dataclass(frozen=True)
class OutboxPruneResult:
    """Structured audit report for an outbox retention pruning run."""

    cutoff_timestamp: float
    records_inspected: int
    records_pruned: int
    records_archived: int
    records_skipped_by_status: int
    records_skipped_by_age: int
    has_more: bool
    error: Optional[str] = None
    pruned_event_ids: Tuple[str, ...] = field(default_factory=tuple)
    records_skipped_in_flight: int = 0

    @property
    def pruned_count(self) -> int:
        return self.records_pruned

    @property
    def archived_count(self) -> int:
        return self.records_archived

    @property
    def inspected_count(self) -> int:
        return self.records_inspected

    @property
    def skipped_count(self) -> int:
        return self.records_skipped_by_status + self.records_skipped_by_age + self.records_skipped_in_flight

    def to_dict(self) -> Dict[str, Any]:
        """Convert prune audit result to a JSON-serializable dictionary."""
        return {
            "cutoff_timestamp": self.cutoff_timestamp,
            "records_inspected": self.records_inspected,
            "records_pruned": self.records_pruned,
            "records_archived": self.records_archived,
            "records_skipped_by_status": self.records_skipped_by_status,
            "records_skipped_by_age": self.records_skipped_by_age,
            "records_skipped_in_flight": self.records_skipped_in_flight,
            "has_more": self.has_more,
            "error": self.error,
            "pruned_event_ids": list(self.pruned_event_ids),
        }


@dataclass(frozen=True)
class OutboxRecord:
    """Durable record representing an event staged for publication via the outbox."""

    event_id: str
    event_type: str
    schema_version: int = 1
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    status: OutboxStatus = OutboxStatus.PENDING
    attempt_count: int = 0
    max_attempts: int = 5
    last_error: Optional[str] = None
    next_retry_at: Optional[float] = None
    delivered_at: Optional[float] = None
    last_attempt_at: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.event_id or not isinstance(self.event_id, str):
            raise ValidationError("OutboxRecord event_id must be a non-empty string.")
        clean_id = self.event_id.strip()
        if not SAFE_EVENT_ID_REGEX.match(clean_id):
            raise ValidationError(
                f"Invalid outbox event_id '{self.event_id}'. Must be alphanumeric with '-' or '_', length 2-128."
            )
        object.__setattr__(self, "event_id", clean_id)

        if not self.event_type or not isinstance(self.event_type, str) or not self.event_type.strip():
            raise ValidationError("OutboxRecord event_type must be a non-empty string.")
        object.__setattr__(self, "event_type", self.event_type.strip())

        if self.schema_version < 1:
            raise ValidationError(f"OutboxRecord schema_version must be >= 1, got {self.schema_version}")

        if not isinstance(self.status, OutboxStatus):
            try:
                object.__setattr__(self, "status", OutboxStatus(str(self.status)))
            except ValueError:
                raise ValidationError(f"Invalid OutboxStatus: {self.status}")

        if self.attempt_count < 0:
            raise ValidationError(f"OutboxRecord attempt_count cannot be negative, got {self.attempt_count}")

        if self.max_attempts <= 0:
            raise ValidationError(f"OutboxRecord max_attempts must be positive, got {self.max_attempts}")

        # Ensure payload is sanitized and deep-copied to guarantee immutability
        sanitized_payload = redact_sensitive_payload(self.payload)
        object.__setattr__(self, "payload", copy.deepcopy(sanitized_payload))

    @property
    def retry_count(self) -> int:
        """Alias for attempt_count."""
        return self.attempt_count

    def to_dict(self) -> Dict[str, Any]:
        """Convert outbox record to a JSON-serializable dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "schema_version": self.schema_version,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "payload": self.payload,
            "created_at": self.created_at,
            "status": self.status.value,
            "attempt_count": self.attempt_count,
            "max_attempts": self.max_attempts,
            "last_error": self.last_error,
            "next_retry_at": self.next_retry_at,
            "delivered_at": self.delivered_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> OutboxRecord:
        """Construct OutboxRecord from a dictionary."""
        status_raw = data.get("status", OutboxStatus.PENDING.value)
        status = OutboxStatus(status_raw) if not isinstance(status_raw, OutboxStatus) else status_raw
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            schema_version=data.get("schema_version", 1),
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
            payload=data.get("payload", {}),
            created_at=data.get("created_at", time.time()),
            status=status,
            attempt_count=data.get("attempt_count", 0),
            max_attempts=data.get("max_attempts", 5),
            last_error=data.get("last_error"),
            next_retry_at=data.get("next_retry_at"),
            delivered_at=data.get("delivered_at"),
        )

    def to_event(self) -> Event:
        """Convert outbox record into a standard immutable KAISER Event for publication."""
        payload_with_meta = dict(self.payload)
        if self.attempt_count > 0:
            payload_with_meta["_outbox_retry"] = True
            payload_with_meta["_outbox_attempt"] = self.attempt_count + 1

        return Event.create(
            event_id=self.event_id,
            event_type=self.event_type,
            source="paper_outbox",
            correlation_id=self.correlation_id or self.event_id,
            payload=payload_with_meta,
            timestamp=self.created_at,
        )


class TransactionalOutbox:
    """Thread-safe, optionally SQLite-persisted Transactional Outbox.

    Stores event records staged during financial operations and manages their
    delivery lifecycle (PENDING -> DELIVERED / RETRYABLE_FAILURE / EXHAUSTED).
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        name: str = "paper_outbox",
        retention_enabled: bool = False,
        retention_days: float = 30.0,
        prune_batch_size: int = 500,
        archival_enabled: bool = False,
    ) -> None:
        self.name = name
        self.db_path = db_path
        self.retention_enabled = retention_enabled
        self.retention_days = float(retention_days)
        self.prune_batch_size = int(prune_batch_size)
        self.archival_enabled = archival_enabled
        self._records: Dict[str, OutboxRecord] = {}
        self._lock = threading.RLock()
        self._recovery_diagnostics: Dict[str, Any] = {
            "recovered_count": 0,
            "corrupt_count": 0,
            "last_recovery_time": None,
        }
        self._total_pruned: int = 0
        self._total_archived: int = 0
        self._last_prune_result: Optional[OutboxPruneResult] = None

        if self.db_path:
            self._init_sqlite_db()
            self.recover()

    # Alias for compatibility
    def stage_event(self, record: OutboxRecord, conn: Optional[sqlite3.Connection] = None) -> bool:
        return self.stage_record(record=record, conn=conn)

    def _init_sqlite_db(self) -> None:
        """Initialize SQLite tables and indexes for outbox persistence and archival."""
        assert self.db_path is not None
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_outbox_entries (
                        event_id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        schema_version INTEGER NOT NULL DEFAULT 1,
                        correlation_id TEXT,
                        causation_id TEXT,
                        payload_json TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        status TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL DEFAULT 5,
                        last_error TEXT,
                        next_retry_at REAL,
                        delivered_at REAL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_outbox_status ON paper_outbox_entries(status)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_outbox_retry ON paper_outbox_entries(next_retry_at)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_outbox_delivered ON paper_outbox_entries(delivered_at)"
                )
                # Historical Archive Table (Phase 2, Step 65)
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_outbox_archive (
                        event_id TEXT PRIMARY KEY,
                        event_type TEXT NOT NULL,
                        schema_version INTEGER NOT NULL DEFAULT 1,
                        correlation_id TEXT,
                        causation_id TEXT,
                        payload_json TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        status TEXT NOT NULL,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        max_attempts INTEGER NOT NULL DEFAULT 5,
                        last_error TEXT,
                        next_retry_at REAL,
                        delivered_at REAL,
                        archived_at REAL NOT NULL,
                        archive_reason TEXT DEFAULT 'retention_prune'
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_outbox_archive_at ON paper_outbox_archive(archived_at)"
                )
        except Exception as err:
            logger.error("Failed to initialize paper outbox SQLite table at %s: %s", self.db_path, err)
            raise

    def _get_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection for internal operations."""
        if not self.db_path:
            raise ValidationError("db_path is not configured for this outbox instance")
        return sqlite3.connect(self.db_path, timeout=10.0)

    def stage_record(
        self,
        record: OutboxRecord,
        conn: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Stage an outbox record.

        If a SQLite connection `conn` is provided, the record is inserted within the
        caller's active transaction. Otherwise, a new connection/transaction is used.

        Returns:
            bool: True if newly staged, False if duplicate event_id was deduplicated.
        """
        with self._lock:
            if record.event_id in self._records:
                logger.debug("Deduplicating outbox record in memory [event_id=%s]", record.event_id)
                return False

            if self.db_path:
                if conn is not None:
                    # Execute within existing database transaction (shared with financial ledger)
                    cursor = conn.execute(
                        """
                        INSERT OR IGNORE INTO paper_outbox_entries (
                            event_id, event_type, schema_version, correlation_id, causation_id,
                            payload_json, created_at, status, attempt_count, max_attempts,
                            last_error, next_retry_at, delivered_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            record.event_id,
                            record.event_type,
                            record.schema_version,
                            record.correlation_id,
                            record.causation_id,
                            json.dumps(record.payload),
                            record.created_at,
                            record.status.value,
                            record.attempt_count,
                            record.max_attempts,
                            record.last_error,
                            record.next_retry_at,
                            record.delivered_at,
                        ),
                    )
                    if cursor.rowcount == 0:
                        return False
                else:
                    try:
                        with sqlite3.connect(self.db_path, timeout=10.0) as local_conn:
                            with local_conn:
                                cursor = local_conn.execute(
                                    """
                                    INSERT OR IGNORE INTO paper_outbox_entries (
                                        event_id, event_type, schema_version, correlation_id, causation_id,
                                        payload_json, created_at, status, attempt_count, max_attempts,
                                        last_error, next_retry_at, delivered_at
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        record.event_id,
                                        record.event_type,
                                        record.schema_version,
                                        record.correlation_id,
                                        record.causation_id,
                                        json.dumps(record.payload),
                                        record.created_at,
                                        record.status.value,
                                        record.attempt_count,
                                        record.max_attempts,
                                        record.last_error,
                                        record.next_retry_at,
                                        record.delivered_at,
                                    ),
                                )
                                if cursor.rowcount == 0:
                                    return False
                    except Exception as err:
                        logger.error("Failed to persist outbox record %s: %s", record.event_id, err)
                        raise

            # Only append to memory if persistence succeeded
            self._records[record.event_id] = record
            return True

    def stage_records(
        self,
        records: Sequence[OutboxRecord],
        conn: Optional[sqlite3.Connection] = None,
    ) -> int:
        """Atomically stage multiple outbox records."""
        with self._lock:
            staged = 0
            for r in records:
                if self.stage_record(r, conn=conn):
                    staged += 1
            return staged

    def mark_delivered(
        self,
        event_id: str,
        delivered_at: Optional[float] = None,
        conn: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Mark an outbox record as DELIVERED durably."""
        with self._lock:
            rec = self._records.get(event_id)
            if rec is None and not self.db_path:
                return False

            now = delivered_at if delivered_at is not None else time.time()

            if self.db_path:
                try:
                    if conn is not None:
                        conn.execute(
                            """
                            UPDATE paper_outbox_entries
                            SET status = ?, delivered_at = ?, next_retry_at = NULL
                            WHERE event_id = ?
                            """,
                            (OutboxStatus.DELIVERED.value, now, event_id),
                        )
                    else:
                        with sqlite3.connect(self.db_path, timeout=10.0) as local_conn:
                            with local_conn:
                                local_conn.execute(
                                    """
                                    UPDATE paper_outbox_entries
                                    SET status = ?, delivered_at = ?, next_retry_at = NULL
                                    WHERE event_id = ?
                                    """,
                                    (OutboxStatus.DELIVERED.value, now, event_id),
                                )
                except Exception as err:
                    logger.error("Failed to mark outbox record %s as delivered in SQLite: %s", event_id, err)
                    raise

            if rec is not None:
                updated = OutboxRecord(
                    event_id=rec.event_id,
                    event_type=rec.event_type,
                    schema_version=rec.schema_version,
                    correlation_id=rec.correlation_id,
                    causation_id=rec.causation_id,
                    payload=rec.payload,
                    created_at=rec.created_at,
                    status=OutboxStatus.DELIVERED,
                    attempt_count=rec.attempt_count + 1,
                    max_attempts=rec.max_attempts,
                    last_error=None,
                    next_retry_at=None,
                    delivered_at=now,
                )
                self._records[event_id] = updated
            return True

    def record_failure(
        self,
        event_id: str,
        error: str,
        next_retry_at: Optional[float] = None,
        exhausted: bool = False,
        conn: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Record a publication failure, updating attempt count, status, and retry schedule."""
        with self._lock:
            rec = self._records.get(event_id)
            if rec is None and not self.db_path:
                return False

            new_status = OutboxStatus.EXHAUSTED if exhausted else OutboxStatus.RETRYABLE_FAILURE
            attempts = (rec.attempt_count + 1) if rec else 1
            max_att = rec.max_attempts if rec else 5
            if attempts >= max_att:
                new_status = OutboxStatus.EXHAUSTED

            if self.db_path:
                try:
                    if conn is not None:
                        conn.execute(
                            """
                            UPDATE paper_outbox_entries
                            SET status = ?, attempt_count = ?, last_error = ?, next_retry_at = ?
                            WHERE event_id = ?
                            """,
                            (new_status.value, attempts, error[:1024], next_retry_at, event_id),
                        )
                    else:
                        with sqlite3.connect(self.db_path, timeout=10.0) as local_conn:
                            with local_conn:
                                local_conn.execute(
                                    """
                                    UPDATE paper_outbox_entries
                                    SET status = ?, attempt_count = ?, last_error = ?, next_retry_at = ?
                                    WHERE event_id = ?
                                    """,
                                    (new_status.value, attempts, error[:1024], next_retry_at, event_id),
                                )
                except Exception as err:
                    logger.error("Failed to record outbox failure for %s in SQLite: %s", event_id, err)
                    raise

            if rec is not None:
                updated = OutboxRecord(
                    event_id=rec.event_id,
                    event_type=rec.event_type,
                    schema_version=rec.schema_version,
                    correlation_id=rec.correlation_id,
                    causation_id=rec.causation_id,
                    payload=rec.payload,
                    created_at=rec.created_at,
                    status=new_status,
                    attempt_count=attempts,
                    max_attempts=rec.max_attempts,
                    last_error=error[:1024],
                    next_retry_at=next_retry_at if new_status != OutboxStatus.EXHAUSTED else None,
                    delivered_at=rec.delivered_at,
                )
                self._records[event_id] = updated
            return True

    def get_record(self, event_id: str) -> Optional[OutboxRecord]:
        """Lookup an outbox record by its event ID."""
        with self._lock:
            return self._records.get(event_id)

    def get_pending_batch(
        self,
        limit: int = 50,
        now: Optional[float] = None,
    ) -> List[OutboxRecord]:
        """Fetch a batch of events eligible for dispatch (PENDING or RETRYABLE_FAILURE ready for retry)."""
        current_time = now if now is not None else time.time()
        with self._lock:
            candidates: List[OutboxRecord] = []
            for r in self._records.values():
                if r.status == OutboxStatus.PENDING:
                    candidates.append(r)
                elif r.status == OutboxStatus.RETRYABLE_FAILURE:
                    if r.next_retry_at is None or r.next_retry_at <= current_time:
                        candidates.append(r)

            # Sort deterministically by created_at ascending
            candidates.sort(key=lambda x: (x.created_at, x.event_id))
            return candidates[:limit]

    def list_records(
        self,
        status: Optional[OutboxStatus] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[OutboxRecord]:
        """List outbox records with optional status filtering and pagination."""
        with self._lock:
            recs = list(self._records.values())
            if status is not None:
                recs = [r for r in recs if r.status == status]
            recs.sort(key=lambda x: (x.created_at, x.event_id))
            return recs[offset : offset + limit]

    def get_stats(self) -> Dict[str, int]:
        """Get aggregate counts of outbox records partitioned by delivery status."""
        with self._lock:
            counts = {
                "pending": 0,
                "delivered": 0,
                "retryable": 0,
                "exhausted": 0,
                "total": len(self._records),
            }
            for r in self._records.values():
                if r.status == OutboxStatus.PENDING:
                    counts["pending"] += 1
                elif r.status == OutboxStatus.DELIVERED:
                    counts["delivered"] += 1
                elif r.status == OutboxStatus.RETRYABLE_FAILURE:
                    counts["retryable"] += 1
                elif r.status == OutboxStatus.EXHAUSTED:
                    counts["exhausted"] += 1
            return counts

    def recover(self) -> Dict[str, Any]:
        """Idempotently recover outbox events from durable SQLite storage.

        Safely handles corrupted JSON payloads by marking them EXHAUSTED rather than
        aborting process recovery. Never duplicates financial ledger entries.
        """
        with self._lock:
            if not self.db_path or not Path(self.db_path).is_file():
                return self._recovery_diagnostics

            recovered = 0
            corrupt = 0
            try:
                with sqlite3.connect(self.db_path, timeout=5.0) as conn:
                    cursor = conn.execute(
                        """
                        SELECT event_id, event_type, schema_version, correlation_id, causation_id,
                               payload_json, created_at, status, attempt_count, max_attempts,
                               last_error, next_retry_at, delivered_at
                        FROM paper_outbox_entries
                        ORDER BY created_at ASC, rowid ASC
                        """
                    )
                    for row in cursor.fetchall():
                        (
                            eid, etype, s_ver, corr_id, caus_id,
                            pay_json, c_at, stat_str, attempts, max_att,
                            l_err, nxt_retry, deliv_at
                        ) = row

                        try:
                            payload = json.loads(pay_json) if pay_json else {}
                            status = OutboxStatus(stat_str)
                        except Exception as json_err:
                            logger.error("Corrupted JSON in outbox record %s: %s", eid, json_err)
                            payload = {"_corrupted_payload": str(pay_json)[:256]}
                            status = OutboxStatus.EXHAUSTED
                            l_err = f"Corruption on recovery: {json_err}"
                            corrupt += 1

                        try:
                            rec = OutboxRecord(
                                event_id=eid,
                                event_type=etype,
                                schema_version=s_ver,
                                correlation_id=corr_id,
                                causation_id=caus_id,
                                payload=payload,
                                created_at=c_at,
                                status=status,
                                attempt_count=attempts,
                                max_attempts=max_att,
                                last_error=l_err,
                                next_retry_at=nxt_retry,
                                delivered_at=deliv_at,
                            )
                            # Idempotent assignment — prevents duplicating identities
                            self._records[eid] = rec
                            recovered += 1
                        except ValidationError as val_err:
                            logger.warning("Skipping invalid outbox record %s during recovery: %s", eid, val_err)
                            corrupt += 1

            except Exception as err:
                logger.error("Failed to recover outbox entries from SQLite: %s", err)
                raise ValidationError(f"Failed to recover outbox entries from SQLite ({self.db_path}): {err}") from err

            self._recovery_diagnostics = {
                "recovered_count": recovered,
                "corrupt_count": corrupt,
                "last_recovery_time": time.time(),
            }
            return dict(self._recovery_diagnostics)

    def prune_delivered_records(
        self,
        retention_seconds: Optional[float] = None,
        batch_size: Optional[int] = None,
        archive: Optional[bool] = None,
        now: Optional[float] = None,
        exclude_event_ids: Optional[Set[str]] = None,
    ) -> OutboxPruneResult:
        """Prune older DELIVERED outbox records whose delivered_at is older than retention_seconds.

        Strict Safety Guarantees:
        - ONLY records with status=DELIVERED are eligible.
        - NEVER prunes PENDING, RETRYABLE_FAILURE, or EXHAUSTED records.
        - Requires a valid, finite delivered_at timestamp.
        - Does NOT substitute creation time for delivered time.
        - Bounded: processes at most batch_size records per invocation (capped at 1000).
        - Atomic: in SQLite, deletion (and optional archive) runs in a single transaction.
          In-memory state is only evicted if the database transaction commits.
        - Excludes any event IDs currently in-flight or specified by the caller.

        Args:
            retention_seconds: Age in seconds since delivery required for pruning (> 0).
                If None, uses self.retention_days * 86400.0.
            batch_size: Maximum records to prune in this batch (> 0, capped at 1000).
                If None, uses self.prune_batch_size.
            archive: If True, copy eligible records to paper_outbox_archive before deletion.
                If None, uses self.archival_enabled.
            now: Optional override for current timestamp (for deterministic testing).
            exclude_event_ids: Set of event IDs to protect from pruning (e.g. in-flight).

        Returns:
            OutboxPruneResult: Comprehensive audit record of the pruning operation.
        """
        sec = float(retention_seconds) if retention_seconds is not None else (self.retention_days * 86400.0)
        bs = int(batch_size) if batch_size is not None else self.prune_batch_size
        do_archive = bool(archive) if archive is not None else self.archival_enabled

        if not math.isfinite(sec) or sec <= 0:
            raise ValidationError(
                f"retention_seconds must be a positive finite float, got {sec}"
            )
        if bs <= 0:
            raise ValidationError(f"batch_size must be a positive integer, got {bs}")

        clamped_batch = min(1000, bs)
        current_time = now if now is not None else time.time()
        cutoff = current_time - sec
        protected_ids = set(exclude_event_ids or ())

        with self._lock:
            records_inspected = 0
            records_skipped_by_status = 0
            records_skipped_by_age = 0
            records_skipped_in_flight = 0
            eligible_ids: List[str] = []

            if self.db_path:
                try:
                    with self._get_connection() as conn:
                        # 1. Inspect status counts for auditability
                        cursor = conn.execute(
                            "SELECT status, COUNT(*) FROM paper_outbox_entries GROUP BY status"
                        )
                        for stat_str, cnt in cursor.fetchall():
                            if stat_str != OutboxStatus.DELIVERED.value:
                                records_skipped_by_status += cnt

                        # 2. Query delivered records
                        cursor = conn.execute(
                            """
                            SELECT event_id, delivered_at FROM paper_outbox_entries
                            WHERE status = ? AND delivered_at IS NOT NULL
                            ORDER BY delivered_at ASC, rowid ASC
                            LIMIT ?
                            """,
                            (OutboxStatus.DELIVERED.value, clamped_batch * 3 + 1),
                        )
                        rows = cursor.fetchall()
                        records_inspected = len(rows)

                        for eid, deliv_at in rows:
                            if eid in protected_ids:
                                records_skipped_in_flight += 1
                                continue
                            if deliv_at is None or not math.isfinite(deliv_at) or deliv_at <= 0.0 or deliv_at > cutoff:
                                records_skipped_by_age += 1
                                continue
                            eligible_ids.append(eid)
                            if len(eligible_ids) >= clamped_batch:
                                break

                        has_more = len(rows) > len(eligible_ids) + records_skipped_by_age + records_skipped_in_flight

                        if not eligible_ids:
                            res = OutboxPruneResult(
                                cutoff_timestamp=cutoff,
                                records_inspected=records_inspected,
                                records_pruned=0,
                                records_archived=0,
                                records_skipped_by_status=records_skipped_by_status,
                                records_skipped_by_age=records_skipped_by_age,
                                records_skipped_in_flight=records_skipped_in_flight,
                                has_more=has_more,
                            )
                            self._last_prune_result = res
                            return res

                        # 3. Atomic SQLite Transaction
                        with conn:
                            placeholders = ",".join("?" for _ in eligible_ids)
                            archived_count = 0
                            if do_archive:
                                conn.execute(
                                    f"""
                                    INSERT OR IGNORE INTO paper_outbox_archive (
                                        event_id, event_type, schema_version, correlation_id, causation_id,
                                        payload_json, created_at, status, attempt_count, max_attempts,
                                        last_error, next_retry_at, delivered_at, archived_at, archive_reason
                                    )
                                    SELECT event_id, event_type, schema_version, correlation_id, causation_id,
                                           payload_json, created_at, status, attempt_count, max_attempts,
                                           last_error, next_retry_at, delivered_at, ?, 'retention_prune'
                                    FROM paper_outbox_entries
                                    WHERE event_id IN ({placeholders})
                                    """,
                                    [current_time] + eligible_ids,
                                )
                                archived_count = len(eligible_ids)

                            conn.execute(
                                f"DELETE FROM paper_outbox_entries WHERE event_id IN ({placeholders})",
                                eligible_ids,
                            )

                        # 4. In-Memory Eviction (only after successful SQLite commit)
                        for eid in eligible_ids:
                            self._records.pop(eid, None)

                        self._total_pruned += len(eligible_ids)
                        self._total_archived += archived_count

                        res = OutboxPruneResult(
                            cutoff_timestamp=cutoff,
                            records_inspected=records_inspected,
                            records_pruned=len(eligible_ids),
                            records_archived=archived_count,
                            records_skipped_by_status=records_skipped_by_status,
                            records_skipped_by_age=records_skipped_by_age,
                            records_skipped_in_flight=records_skipped_in_flight,
                            has_more=has_more,
                            pruned_event_ids=tuple(eligible_ids),
                        )
                        self._last_prune_result = res
                        logger.info(
                            "Outbox pruned %d records (archived=%d, cutoff=%.2f, has_more=%s)",
                            len(eligible_ids),
                            archived_count,
                            cutoff,
                            has_more,
                        )
                        return res

                except Exception as err:
                    logger.error("Failed to prune delivered outbox records in SQLite: %s", err)
                    raise

            else:
                # In-memory only outbox
                records_inspected = len(self._records)
                delivered_candidates = []
                for r in self._records.values():
                    if r.status != OutboxStatus.DELIVERED:
                        records_skipped_by_status += 1
                        continue
                    if r.event_id in protected_ids:
                        continue
                    if r.delivered_at is None or not math.isfinite(r.delivered_at) or r.delivered_at > cutoff:
                        records_skipped_by_age += 1
                        continue
                    delivered_candidates.append(r)

                delivered_candidates.sort(key=lambda r: r.delivered_at or 0.0)
                to_prune_recs = delivered_candidates[:clamped_batch]
                has_more = len(delivered_candidates) > len(to_prune_recs)

                pruned_eids = [r.event_id for r in to_prune_recs]
                for eid in pruned_eids:
                    self._records.pop(eid, None)

                archived_count = len(pruned_eids) if archive else 0
                self._total_pruned += len(pruned_eids)
                self._total_archived += archived_count

                res = OutboxPruneResult(
                    cutoff_timestamp=cutoff,
                    records_inspected=records_inspected,
                    records_pruned=len(pruned_eids),
                    records_archived=archived_count,
                    records_skipped_by_status=records_skipped_by_status,
                    records_skipped_by_age=records_skipped_by_age,
                    has_more=has_more,
                    pruned_event_ids=tuple(pruned_eids),
                )
                self._last_prune_result = res
                return res

    def preview_prune_delivered_records(
        self,
        retention_seconds: Optional[float] = None,
        batch_size: Optional[int] = None,
        now: Optional[float] = None,
    ) -> OutboxPruneResult:
        """Inspect and return what would be pruned without mutating any database or in-memory records."""
        sec = float(retention_seconds) if retention_seconds is not None else (self.retention_days * 86400.0)
        bs = int(batch_size) if batch_size is not None else self.prune_batch_size

        if not math.isfinite(sec) or sec <= 0:
            raise ValidationError(
                f"retention_seconds must be a positive finite float, got {sec}"
            )
        if bs <= 0:
            raise ValidationError(f"batch_size must be a positive integer, got {bs}")

        clamped_batch = min(1000, bs)
        current_time = now if now is not None else time.time()
        cutoff = current_time - sec

        with self._lock:
            eligible_ids: List[str] = []
            records_skipped_by_status = 0
            records_skipped_by_age = 0

            for r in self._records.values():
                if r.status != OutboxStatus.DELIVERED:
                    records_skipped_by_status += 1
                    continue
                if r.delivered_at is None or not math.isfinite(r.delivered_at) or r.delivered_at <= 0.0 or r.delivered_at > cutoff:
                    records_skipped_by_age += 1
                    continue
                eligible_ids.append(r.event_id)

            eligible_ids.sort(key=lambda eid: self._records[eid].delivered_at or 0.0)
            preview_batch = eligible_ids[:clamped_batch]
            has_more = len(eligible_ids) > len(preview_batch)

            return OutboxPruneResult(
                cutoff_timestamp=cutoff,
                records_inspected=len(self._records),
                records_pruned=0,
                records_archived=0,
                records_skipped_by_status=records_skipped_by_status,
                records_skipped_by_age=records_skipped_by_age,
                has_more=has_more,
                pruned_event_ids=tuple(preview_batch),
            )

    def get_retention_status(self) -> Dict[str, Any]:
        """Summary of outbox retention metrics and last prune results."""
        with self._lock:
            return {
                "total_pruned": self._total_pruned,
                "total_archived": self._total_archived,
                "total_records": len(self._records),
                "last_prune": self._last_prune_result.to_dict() if self._last_prune_result else None,
            }

    def get_archived_record(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve an archived outbox record by event ID if present in SQLite."""
        if not self.db_path:
            return None
        with self._lock:
            try:
                with self._get_connection() as conn:
                    cursor = conn.execute(
                        """
                        SELECT event_id, event_type, schema_version, correlation_id, causation_id,
                               payload_json, created_at, status, attempt_count, max_attempts,
                               last_error, next_retry_at, delivered_at, archived_at, archive_reason
                        FROM paper_outbox_archive
                        WHERE event_id = ?
                        """,
                        (event_id,),
                    )
                    row = cursor.fetchone()
                    if not row:
                        return None
                    try:
                        payload = json.loads(row[5]) if row[5] else {}
                    except Exception:
                        payload = {}
                    return {
                        "event_id": row[0],
                        "event_type": row[1],
                        "schema_version": row[2],
                        "correlation_id": row[3],
                        "causation_id": row[4],
                        "payload": payload,
                        "created_at": row[6],
                        "status": row[7],
                        "attempt_count": row[8],
                        "max_attempts": row[9],
                        "last_error": row[10],
                        "next_retry_at": row[11],
                        "delivered_at": row[12],
                        "archived_at": row[13],
                        "archive_reason": row[14],
                    }
            except Exception as err:
                logger.error("Failed to fetch archived outbox record %s: %s", event_id, err)
                return None

    def prune_delivered(self, keep_last_n: int = 1000) -> int:
        """Bounded retention: Prune older DELIVERED records, preserving at least keep_last_n.

        NEVER prunes undelivered records (PENDING, RETRYABLE_FAILURE, EXHAUSTED).
        """
        with self._lock:
            delivered_ids = [
                r.event_id for r in self._records.values() if r.status == OutboxStatus.DELIVERED
            ]
            delivered_ids.sort(key=lambda eid: self._records[eid].delivered_at or 0.0)

            excess = len(delivered_ids) - keep_last_n
            if excess <= 0:
                return 0

            to_prune = delivered_ids[:excess]

            if self.db_path:
                try:
                    with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                        with conn:
                            placeholders = ",".join("?" for _ in to_prune)
                            conn.execute(
                                f"DELETE FROM paper_outbox_entries WHERE event_id IN ({placeholders})",
                                to_prune,
                            )
                except Exception as err:
                    logger.error("Failed to prune delivered outbox records in SQLite: %s", err)
                    raise

            for eid in to_prune:
                self._records.pop(eid, None)

            self._total_pruned += len(to_prune)
            return len(to_prune)

    def _get_connection(self) -> sqlite3.Connection:
        """Get an active SQLite connection for outbox persistence."""
        if not self.db_path:
            raise RuntimeError("Database path not configured.")
        return sqlite3.connect(self.db_path, timeout=10.0)

    def count(self) -> int:
        """Total records in the outbox."""
        with self._lock:
            return len(self._records)

    def count_records(self, status: Optional[OutboxStatus] = None) -> int:
        """Total records in the outbox, optionally filtered by status."""
        with self._lock:
            if status is None:
                return len(self._records)
            return sum(1 for r in self._records.values() if r.status == status)


class DeduplicatingEventHandler:
    """Thread-safe consumer decorator/wrapper for idempotent EventBus message handling.

    Maintains a bounded FIFO cache of processed event IDs to prevent duplicate
    processing during outbox retry redeliveries.
    """

    def __init__(
        self,
        handler: Callable[[Event], Any],
        capacity: int = 1000,
    ) -> None:
        self.handler = handler
        self.capacity = max(10, capacity)
        self._seen_ids: Set[str] = set()
        self._id_order: List[str] = []
        self._lock = threading.Lock()
        self._processed_count: int = 0
        self._duplicate_count: int = 0

    @property
    def processed_count(self) -> int:
        with self._lock:
            return self._processed_count

    @property
    def duplicate_count(self) -> int:
        with self._lock:
            return self._duplicate_count

    def __call__(self, event: Event) -> Any:
        eid = event.event_id
        with self._lock:
            if eid in self._seen_ids:
                self._duplicate_count += 1
                logger.debug("Idempotent consumer deduplicated event [id=%s, type=%s]", eid, event.event_type)
                return None

            self._seen_ids.add(eid)
            self._id_order.append(eid)
            if len(self._id_order) > self.capacity:
                oldest = self._id_order.pop(0)
                self._seen_ids.discard(oldest)

            self._processed_count += 1

        return self.handler(event)
