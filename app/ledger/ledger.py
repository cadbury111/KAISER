"""KAISER — Paper Trading Ledger Implementation.

Provides an in-memory, thread-safe, and optionally SQLite-persisted ledger
recording all paper orders, fills, rejections, fee assessments, and cash adjustments.
Guarantees idempotency via stable unique idempotency keys.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from app.core.exceptions import ValidationError
from app.ledger.models import LedgerEntryType, PaperLedgerEntry

logger = logging.getLogger("ledger.paper")


class ReconciliationResult(dict):
    """Dictionary holding reconciliation metrics, supporting tuple unpacking."""

    def __iter__(self):
        # Allow unpacking as: is_balanced, computed_balance, discrepancy
        return iter((self["is_balanced"], self["computed_balance"], self["discrepancy"]))


class PaperLedger:
    """Immutable, append-only, idempotent ledger for paper trading transactions."""

    def __init__(
        self,
        db_path: Optional[str] = None,
        event_bus: Optional[Any] = None,
        initial_balance: Optional[float] = None,
        name: str = "paper_ledger",
    ) -> None:
        self.name = name
        self.db_path = db_path
        self.event_bus = event_bus
        self.initial_balance = initial_balance
        self._entries: List[PaperLedgerEntry] = []
        self._by_idempotency_key: Dict[str, PaperLedgerEntry] = {}
        self._by_order_id: Dict[str, List[PaperLedgerEntry]] = {}
        self._by_entry_id: Dict[str, PaperLedgerEntry] = {}
        self._lock = threading.RLock()

        if self.db_path:
            self._init_sqlite_db()
            self._load_from_sqlite()

        if self.initial_balance is not None and not self._entries:
            self._record_initial_capital(self.initial_balance)

    def _init_sqlite_db(self) -> None:
        """Initialize SQLite table for paper ledger persistence if db_path is provided."""
        assert self.db_path is not None
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_ledger_entries (
                        entry_id TEXT PRIMARY KEY,
                        timestamp REAL NOT NULL,
                        entry_type TEXT NOT NULL,
                        order_id TEXT,
                        client_order_id TEXT,
                        symbol TEXT,
                        side TEXT,
                        quantity REAL,
                        price REAL,
                        fee REAL NOT NULL,
                        cash_change REAL NOT NULL,
                        cash_balance_after REAL NOT NULL,
                        realized_pnl REAL NOT NULL,
                        idempotency_key TEXT UNIQUE NOT NULL,
                        provenance_id TEXT,
                        correlation_id TEXT,
                        metadata_json TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_ledger_order_id ON paper_ledger_entries(order_id)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_ledger_idempotency ON paper_ledger_entries(idempotency_key)"
                )
        except Exception as err:
            logger.error("Failed to initialize paper ledger SQLite database at %s: %s", self.db_path, err)

    def _load_from_sqlite(self) -> None:
        """Load past ledger entries from SQLite database if it exists."""
        if not self.db_path or not Path(self.db_path).is_file():
            return
        try:
            with sqlite3.connect(self.db_path, timeout=5.0) as conn:
                cursor = conn.execute(
                    """
                    SELECT entry_id, timestamp, entry_type, order_id, client_order_id,
                           symbol, side, quantity, price, fee, cash_change,
                           cash_balance_after, realized_pnl, idempotency_key,
                           provenance_id, correlation_id, metadata_json
                    FROM paper_ledger_entries
                    ORDER BY timestamp ASC, rowid ASC
                    """
                )
                for row in cursor.fetchall():
                    (
                        entry_id, ts, etype, order_id, client_oid,
                        symbol, side, qty, price, fee, cash_chg,
                        cash_after, pnl, idemp_key, prov_id, corr_id, meta_json
                    ) = row
                    
                    try:
                        parsed_meta = json.loads(meta_json) if meta_json else {}
                    except Exception as json_err:
                        raise ValidationError(f"Corrupted metadata JSON in ledger entry {entry_id}: {json_err}") from json_err

                    entry = PaperLedgerEntry(
                        entry_id=entry_id,
                        timestamp=ts,
                        entry_type=LedgerEntryType(etype),
                        order_id=order_id,
                        client_order_id=client_oid,
                        symbol=symbol,
                        side=side,
                        quantity=qty or 0.0,
                        price=price or 0.0,
                        fee=fee or 0.0,
                        cash_change=cash_chg,
                        cash_balance_after=cash_after,
                        realized_pnl=pnl or 0.0,
                        idempotency_key=idemp_key,
                        provenance_id=prov_id,
                        correlation_id=corr_id,
                        metadata=parsed_meta,
                    )
                    self._entries.append(entry)
                    self._by_idempotency_key[entry.idempotency_key] = entry
                    self._by_entry_id[entry.entry_id] = entry
                    if entry.order_id:
                        if entry.order_id not in self._by_order_id:
                            self._by_order_id[entry.order_id] = []
                        self._by_order_id[entry.order_id].append(entry)
        except ValidationError:
            raise
        except Exception as err:
            logger.error("Failed to load ledger entries from SQLite: %s", err)
            raise ValidationError(f"Failed to load ledger entries from SQLite ({self.db_path}): {err}") from err

    def _record_initial_capital(self, balance: float) -> None:
        """Record initial capital allocation entry in ledger."""
        entry = PaperLedgerEntry(
            entry_id=f"init_{int(time.time() * 1000)}",
            timestamp=time.time(),
            entry_type=LedgerEntryType.INITIAL_CAPITAL,
            order_id=None,
            client_order_id=None,
            symbol=None,
            side=None,
            quantity=0.0,
            price=0.0,
            fee=0.0,
            cash_change=balance,
            cash_balance_after=balance,
            realized_pnl=0.0,
            idempotency_key=f"init_capital_{self.name}_{balance}",
            metadata={"description": "Initial paper capital allocation"},
        )
        self.record_entry(entry)

    def record_entry(self, entry: PaperLedgerEntry) -> bool:
        """Record an entry in the ledger.

        Args:
            entry: The PaperLedgerEntry to append.

        Returns:
            bool: True if entry was newly recorded, False if deduplicated via idempotency_key.
        """
        with self._lock:
            # 1. In-Memory Idempotency Check
            if entry.idempotency_key in self._by_idempotency_key:
                logger.debug(
                    "Deduplicating ledger entry in-memory [idempotency_key=%s, entry_id=%s]",
                    entry.idempotency_key,
                    entry.entry_id,
                )
                return False

            # 2. SQLite Persistence (Atomic Transaction)
            if self.db_path:
                try:
                    with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                        with conn:  # Transaction context manager commits or rolls back
                            cursor = conn.execute(
                                """
                                INSERT OR IGNORE INTO paper_ledger_entries (
                                    entry_id, timestamp, entry_type, order_id, client_order_id,
                                    symbol, side, quantity, price, fee, cash_change,
                                    cash_balance_after, realized_pnl, idempotency_key,
                                    provenance_id, correlation_id, metadata_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    entry.entry_id,
                                    entry.timestamp,
                                    entry.entry_type.value,
                                    entry.order_id,
                                    entry.client_order_id,
                                    entry.symbol,
                                    entry.side,
                                    entry.quantity,
                                    entry.price,
                                    entry.fee,
                                    entry.cash_change,
                                    entry.cash_balance_after,
                                    entry.realized_pnl,
                                    entry.idempotency_key,
                                    entry.provenance_id,
                                    entry.correlation_id,
                                    json.dumps(entry.metadata),
                                ),
                            )
                            if cursor.rowcount == 0:
                                # SQLite already had this entry (deduplicated)
                                return False
                except Exception as db_err:
                    logger.error("Failed to persist ledger entry %s to SQLite: %s", entry.entry_id, db_err)
                    raise

            # 3. Append to In-Memory Indexes (only after successful persistence)
            self._entries.append(entry)
            self._by_idempotency_key[entry.idempotency_key] = entry
            self._by_entry_id[entry.entry_id] = entry

            if entry.order_id:
                if entry.order_id not in self._by_order_id:
                    self._by_order_id[entry.order_id] = []
                self._by_order_id[entry.order_id].append(entry)

            # 4. Publish Event if EventBus attached
            if self.event_bus:
                try:
                    from app.core.events import Event, EventType
                    self.event_bus.publish(
                        Event.create(
                            event_type=EventType.PAPER_LEDGER_ENTRY_RECORDED,
                            source="paper_ledger",
                            correlation_id=entry.correlation_id or entry.entry_id,
                            payload=entry.to_dict(),
                        )
                    )
                except Exception as err:
                    logger.error("Failed to publish ledger event: %s", err)

            return True

    def record_entries(self, entries: List[PaperLedgerEntry]) -> int:
        """Atomically record multiple ledger entries in a single SQLite transaction.

        Returns:
            int: Number of new entries recorded.
        """
        with self._lock:
            new_entries = [e for e in entries if e.idempotency_key not in self._by_idempotency_key]
            if not new_entries:
                return 0

            if self.db_path:
                try:
                    with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                        with conn:
                            for entry in new_entries:
                                conn.execute(
                                    """
                                    INSERT OR IGNORE INTO paper_ledger_entries (
                                        entry_id, timestamp, entry_type, order_id, client_order_id,
                                        symbol, side, quantity, price, fee, cash_change,
                                        cash_balance_after, realized_pnl, idempotency_key,
                                        provenance_id, correlation_id, metadata_json
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        entry.entry_id,
                                        entry.timestamp,
                                        entry.entry_type.value,
                                        entry.order_id,
                                        entry.client_order_id,
                                        entry.symbol,
                                        entry.side,
                                        entry.quantity,
                                        entry.price,
                                        entry.fee,
                                        entry.cash_change,
                                        entry.cash_balance_after,
                                        entry.realized_pnl,
                                        entry.idempotency_key,
                                        entry.provenance_id,
                                        entry.correlation_id,
                                        json.dumps(entry.metadata),
                                    ),
                                )
                except Exception as db_err:
                    logger.error("Failed atomic batch persistence to SQLite: %s", db_err)
                    raise

            # Update in-memory
            recorded_count = 0
            for entry in new_entries:
                self._entries.append(entry)
                self._by_idempotency_key[entry.idempotency_key] = entry
                self._by_entry_id[entry.entry_id] = entry
                if entry.order_id:
                    if entry.order_id not in self._by_order_id:
                        self._by_order_id[entry.order_id] = []
                    self._by_order_id[entry.order_id].append(entry)
                recorded_count += 1

            return recorded_count

    def get_entries(
        self,
        order_id: Optional[str] = None,
        symbol: Optional[str] = None,
        entry_type: Optional[LedgerEntryType] = None,
        limit: int = 100,
    ) -> List[PaperLedgerEntry]:
        """Query ledger entries filtered by criteria."""
        with self._lock:
            results = list(self._entries)
            if order_id:
                results = [e for e in results if e.order_id == order_id]
            if symbol:
                norm_sym = symbol.strip().upper()
                results = [e for e in results if e.symbol and e.symbol.upper() == norm_sym]
            if entry_type:
                results = [e for e in results if e.entry_type == entry_type]

            return results[-limit:]

    def get_entry(self, entry_id: str) -> Optional[PaperLedgerEntry]:
        """Lookup an entry by its unique entry_id."""
        with self._lock:
            return self._by_entry_id.get(entry_id)

    def get_entry_by_idempotency_key(self, key: str) -> Optional[PaperLedgerEntry]:
        """Lookup an entry by idempotency key."""
        with self._lock:
            return self._by_idempotency_key.get(key)

    def reconcile(
        self,
        initial_cash: Optional[float] = None,
        expected_current_cash: Optional[float] = None,
    ) -> ReconciliationResult:
        """Reconcile ledger cash flows against portfolio cash balance.

        Returns:
            ReconciliationResult: Dict-like result supporting keys and unpacking.
        """
        with self._lock:
            # Check for INITIAL_CAPITAL entry in the ledger
            init_entries = [e for e in self._entries if e.entry_type == LedgerEntryType.INITIAL_CAPITAL]
            has_init_entry = len(init_entries) > 0

            init_mismatch = False
            if has_init_entry:
                base_cash = sum(e.cash_change for e in init_entries)
                non_init_flow = sum(e.cash_change for e in self._entries if e.entry_type != LedgerEntryType.INITIAL_CAPITAL)
                computed_balance = round(base_cash + non_init_flow, 4)
                if initial_cash is not None and abs(initial_cash - base_cash) >= 0.0001:
                    init_mismatch = True
            else:
                base_cash = initial_cash if initial_cash is not None else (self.initial_balance or 0.0)
                total_flow = sum(e.cash_change for e in self._entries)
                computed_balance = round(base_cash + total_flow, 4)

            exp_c = expected_current_cash if expected_current_cash is not None else computed_balance
            diff = round(abs(computed_balance - exp_c), 4)

            # Check cash chain consistency: running cash_change should match cash_balance_after
            chain_broken = False
            running_cash = 0.0 if has_init_entry else base_cash
            for e in self._entries:
                running_cash = round(running_cash + e.cash_change, 4)
                if abs(running_cash - e.cash_balance_after) >= 0.0001:
                    chain_broken = True
                    break

            is_matched = (diff < 0.0001) and (not init_mismatch) and (not chain_broken)
            if init_mismatch and initial_cash is not None:
                diff = max(diff, round(abs(initial_cash - base_cash), 4))

            return ReconciliationResult(
                is_balanced=is_matched,
                discrepancy=diff,
                computed_balance=computed_balance,
                expected_balance=exp_c,
                total_entries=len(self._entries),
            )

    def count(self) -> int:
        """Return total number of entries in the ledger."""
        with self._lock:
            return len(self._entries)
