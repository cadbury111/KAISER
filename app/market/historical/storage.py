"""KAISER — Historical Market Data Local Storage & SQLite Repository.

Provides thread-safe local persistence, bounded querying, uniqueness constraints,
integrity verification, and transactional operations for historical OHLCV bars.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from app.analysis.models import SUPPORTED_TIMEFRAMES, normalize_timeframe
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.logging_config import get_logger
from app.market.historical.models import (
    HistoricalBarRequest,
    HistoricalBarResponse,
    HistoricalProviderStatus,
)
from app.market.historical.provider import HistoricalDataProvider
from app.market.models import Bar

logger = get_logger("market.historical.storage")


class HistoricalBarRepository(ABC):
    """Abstract contract for storing and querying historical market bars."""

    @abstractmethod
    def store_bars(self, bars: Sequence[Bar], source_provider: str = "unknown") -> int:
        """Persist a batch of historical bars. Returns count of bars inserted/updated."""
        pass

    @abstractmethod
    def query_bars(
        self,
        symbol: str,
        timeframe: str,
        start_time: float,
        end_time: float,
        limit: Optional[int] = 1000,
    ) -> Tuple[Bar, ...]:
        """Query stored bars bounded by symbol, timeframe, and timestamp range."""
        pass

    @abstractmethod
    def get_bar_count(
        self,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> int:
        """Return total count of stored bars matching filters."""
        pass

    @abstractmethod
    def get_stored_ranges(
        self,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return summary of stored ranges per (symbol, timeframe)."""
        pass

    @abstractmethod
    def delete_bars(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> int:
        """Delete bars matching criteria. Returns count of deleted rows."""
        pass


class SQLiteHistoricalBarRepository(HistoricalDataProvider, HistoricalBarRepository):
    """Thread-safe SQLite implementation of HistoricalBarRepository.

    Also conforms to HistoricalDataProvider, allowing the repository to be plugged
    directly into HistoricalDataLoader and HistoricalReplayEngine.
    """

    SCHEMA_VERSION = 1

    def __init__(
        self,
        db_path: Union[str, Path] = "data/kaiser.db",
        name: str = "sqlite_historical_repository",
    ) -> None:
        super().__init__(name=name)
        self.db_path_str = str(db_path)
        self._is_memory = self.db_path_str == ":memory:"
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None

        if not self._is_memory:
            # Ensure parent directory exists
            p = Path(self.db_path_str)
            p.parent.mkdir(parents=True, exist_ok=True)

        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """Obtain thread-safe connection to SQLite."""
        if self._is_memory:
            # For in-memory DB, reuse the same connection to persist data
            if self._conn is None:
                self._conn = sqlite3.connect(":memory:", check_same_thread=False)
                self._conn.row_factory = sqlite3.Row
            return self._conn

        # For file DB, create or reuse thread-safe connection
        if self._conn is None:
            conn = sqlite3.connect(self.db_path_str, check_same_thread=False, timeout=10.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            self._conn = conn
        return self._conn

    def _init_database(self) -> None:
        """Initialize tables, indices, and schema version."""
        with self._lock:
            conn = self._get_connection()
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS schema_meta (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS historical_bars (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        exchange TEXT NOT NULL DEFAULT 'NSE',
                        timeframe TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume REAL NOT NULL DEFAULT 0.0,
                        source TEXT NOT NULL DEFAULT 'unknown',
                        is_zero_volume INTEGER NOT NULL DEFAULT 0,
                        quality_status TEXT NOT NULL DEFAULT 'VALID',
                        created_at REAL NOT NULL,
                        UNIQUE(symbol, timeframe, timestamp)
                    );
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_historical_bars_query
                    ON historical_bars (symbol, timeframe, timestamp ASC);
                """)
                conn.execute("""
                    INSERT OR IGNORE INTO schema_meta (key, value)
                    VALUES ('schema_version', ?);
                """, (str(self.SCHEMA_VERSION),))

    def verify_integrity(self) -> bool:
        """Run SQLite integrity check to detect file corruption."""
        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute("PRAGMA integrity_check;")
            row = cursor.fetchone()
            if row and row[0] == "ok":
                return True
            logger.error("SQLite integrity check failed for %s: %s", self.db_path_str, row[0] if row else "None")
            return False

    def store_bars(
        self,
        bars: Sequence[Bar],
        source_provider: str = "unknown",
        on_conflict: str = "replace",
    ) -> int:
        """Persist a collection of Bar records within a single transaction.

        Args:
            bars: Sequence of validated Bar records.
            source_provider: Originating provider identifier.
            on_conflict: 'replace' to update existing records, or 'ignore' to skip duplicates.

        Returns:
            int: Number of rows inserted or updated.
        """
        if not bars:
            return 0

        now = time.time()
        rows_to_insert: List[Tuple[Any, ...]] = []

        for b in bars:
            # Validate bar invariants before inserting
            if not isinstance(b, Bar):
                raise ValidationError(f"Expected Bar instance, got {type(b).__name__}")

            is_zero_vol = 1 if b.volume == 0.0 else 0
            norm_tf = normalize_timeframe(b.timeframe)
            source_tag = b.source if b.source != "mock" else source_provider

            rows_to_insert.append((
                b.symbol.strip().upper(),
                b.exchange.strip().upper(),
                norm_tf,
                float(b.timestamp),
                float(b.open),
                float(b.high),
                float(b.low),
                float(b.close),
                float(b.volume),
                source_tag,
                is_zero_vol,
                "VALID" if is_zero_vol == 0 else "DEGRADED",
                now,
            ))

        sql_action = "INSERT OR REPLACE" if on_conflict.lower() == "replace" else "INSERT OR IGNORE"
        sql = f"""
            {sql_action} INTO historical_bars (
                symbol, exchange, timeframe, timestamp,
                open, high, low, close, volume,
                source, is_zero_volume, quality_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """

        with self._lock:
            conn = self._get_connection()
            try:
                with conn:
                    cursor = conn.executemany(sql, rows_to_insert)
                    inserted = cursor.rowcount if cursor.rowcount >= 0 else len(rows_to_insert)
                logger.info(
                    "Stored %d historical bars for %s (%s) in SQLite repository",
                    inserted,
                    bars[0].symbol if bars else "N/A",
                    bars[0].timeframe if bars else "N/A",
                )
                return inserted
            except sqlite3.Error as err:
                logger.error("Failed to store historical bars in SQLite: %s", err)
                raise ValidationError(f"Database error storing historical bars: {err}") from err

    def query_bars(
        self,
        symbol: str,
        timeframe: str,
        start_time: float,
        end_time: float,
        limit: Optional[int] = 1000,
    ) -> Tuple[Bar, ...]:
        """Retrieve stored bars in strictly ascending chronological order."""
        if not symbol or not isinstance(symbol, str) or not symbol.strip():
            raise ValidationError("Query symbol must be a non-empty string.")
        norm_sym = symbol.strip().upper()
        norm_tf = normalize_timeframe(timeframe)

        if start_time <= 0 or end_time <= 0 or end_time < start_time:
            raise ValidationError(
                f"Invalid query time range: start={start_time}, end={end_time}"
            )

        max_rows = min(limit or 1000, 5000)

        sql = """
            SELECT symbol, exchange, timeframe, timestamp, open, high, low, close, volume, source
            FROM historical_bars
            WHERE symbol = ? AND timeframe = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
            LIMIT ?;
        """

        with self._lock:
            conn = self._get_connection()
            try:
                cursor = conn.execute(sql, (norm_sym, norm_tf, float(start_time), float(end_time), max_rows))
                rows = cursor.fetchall()
            except sqlite3.Error as err:
                logger.error("SQLite query error: %s", err)
                raise ValidationError(f"Database error querying historical bars: {err}") from err

        result_bars: List[Bar] = []
        for r in rows:
            try:
                bar = Bar(
                    symbol=r["symbol"],
                    exchange=r["exchange"],
                    timeframe=r["timeframe"],
                    timestamp=float(r["timestamp"]),
                    open=float(r["open"]),
                    high=float(r["high"]),
                    low=float(r["low"]),
                    close=float(r["close"]),
                    volume=float(r["volume"]),
                    source=r["source"],
                )
                result_bars.append(bar)
            except ValidationError as v_err:
                logger.error("Corrupted record detected in SQLite database: %s", v_err)
                raise ValidationError(f"Corrupted bar record in database at ts={r['timestamp']}: {v_err}") from v_err

        return tuple(result_bars)

    def get_bar_count(
        self,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> int:
        """Return total number of stored bars matching filters."""
        query = "SELECT COUNT(*) FROM historical_bars WHERE 1=1"
        params: List[Any] = []

        if symbol:
            query += " AND symbol = ?"
            params.append(symbol.strip().upper())
        if timeframe:
            query += " AND timeframe = ?"
            params.append(normalize_timeframe(timeframe))

        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(query, params)
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def get_stored_ranges(
        self,
        symbol: Optional[str] = None,
        timeframe: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return summary of stored date ranges grouped by (symbol, timeframe)."""
        query = """
            SELECT
                symbol,
                timeframe,
                COUNT(*) as bar_count,
                MIN(timestamp) as min_timestamp,
                MAX(timestamp) as max_timestamp,
                SUM(is_zero_volume) as zero_volume_count
            FROM historical_bars
            WHERE 1=1
        """
        params: List[Any] = []
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol.strip().upper())
        if timeframe:
            query += " AND timeframe = ?"
            params.append(normalize_timeframe(timeframe))

        query += " GROUP BY symbol, timeframe ORDER BY symbol, timeframe ASC;"

        with self._lock:
            conn = self._get_connection()
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        ranges: List[Dict[str, Any]] = []
        for r in rows:
            min_ts = r["min_timestamp"]
            max_ts = r["max_timestamp"]
            ranges.append({
                "symbol": r["symbol"],
                "timeframe": r["timeframe"],
                "bar_count": r["bar_count"],
                "zero_volume_count": r["zero_volume_count"],
                "min_timestamp": min_ts,
                "max_timestamp": max_ts,
                "min_iso": datetime.fromtimestamp(min_ts, tz=timezone.utc).isoformat() if min_ts else None,
                "max_iso": datetime.fromtimestamp(max_ts, tz=timezone.utc).isoformat() if max_ts else None,
            })
        return ranges

    def delete_bars(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> int:
        """Delete bars matching stream and optional timestamp range."""
        norm_sym = symbol.strip().upper()
        norm_tf = normalize_timeframe(timeframe)

        query = "DELETE FROM historical_bars WHERE symbol = ? AND timeframe = ?"
        params: List[Any] = [norm_sym, norm_tf]

        if start_time is not None:
            query += " AND timestamp >= ?"
            params.append(float(start_time))
        if end_time is not None:
            query += " AND timestamp <= ?"
            params.append(float(end_time))

        with self._lock:
            conn = self._get_connection()
            with conn:
                cursor = conn.execute(query, params)
                return cursor.rowcount

    # --------------------------------------------------------------------------
    # HistoricalDataProvider Implementation
    # --------------------------------------------------------------------------

    def get_historical_bars(self, request: HistoricalBarRequest) -> HistoricalBarResponse:
        """Retrieve stored bars conforming to HistoricalDataProvider interface."""
        bars = self.query_bars(
            symbol=request.symbol,
            timeframe=request.timeframe,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit or 1000,
        )
        status = HistoricalProviderStatus.SUCCESS if bars else HistoricalProviderStatus.EMPTY
        return HistoricalBarResponse(
            request=request,
            status=status,
            bars=bars,
            provider_name=self.name,
        )

    def close(self) -> None:
        """Close database connection cleanly."""
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None
