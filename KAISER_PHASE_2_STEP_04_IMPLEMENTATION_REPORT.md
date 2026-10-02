# KAISER — Phase 2, Step 04: Historical Data Storage, Cache & Replay Engine Implementation Report

## Executive Summary

Phase 2, Step 04 delivers thread-safe local persistence, bounded in-memory LRU caching, and a deterministic historical market data replay engine for the KAISER intelligence platform. This step builds directly upon the provider foundation established in Step 03, integrating seamlessly with the existing `HistoricalDataLoader`, `MarketDataQualityEngine`, `TechnicalAnalysisEngine`, `EventBus`, `StateStore`, and `MasterOrchestrator`.

All operations remain strictly offline and safe: no broker connections or automated trading capabilities were created, and execution remains locked (`Settings.execution_locked = True`).

---

## 1. Architectural Design & Component Overview

```
+---------------------------------------------------------------------------------------+
|                                  MasterOrchestrator                                   |
|   Commands: load_historical_data | store_historical_bars                              |
|             query_historical_bars | replay_historical_bars                            |
+---------------------------------------------------------------------------------------+
                |                                      |
                v                                      v
+-------------------------------+      +------------------------------------------------+
|     HistoricalDataLoader      |      |             HistoricalReplayEngine             |
| - load_bars()                 |      | - replay(HistoricalReplayRequest)              |
| - persist_bars()              |      | - cancel_replay(replay_id)                     |
| - Cache-aside integration     |      | - Chronological playback loop                  |
+-------------------------------+      | - Configurable pacing (immediate or paced)     |
        |              |               | - State isolation (reset_stream)               |
        |              |               | - Lifecycle events (STARTED, PROGRESS, etc.)   |
        v              v               +------------------------------------------------+
+--------------+ +--------------+                      |                       |
|   Bounded    | |    SQLite    |<---------------------+                       |
|  LRU Cache   | |  Repository  |                                              v
| (Thread-safe)| | (WAL mode,   |                              +-------------------------------+
| TTL, Evict,  | |  Unique idx, |                              |    MarketDataQualityEngine    |
| Invalidation | |  Integrity)  |                              | - evaluate_bar(candle_ts)     |
+--------------+ +--------------+                              +-------------------------------+
                                                                               | (safe bars)
                                                                               v
                                                               +-------------------------------+
                                                               |    TechnicalAnalysisEngine    |
                                                               | - RollingBarSeries window     |
                                                               | - Indicator feature vectors   |
                                                               +-------------------------------+
```

---

## 2. Key Components Implemented

### 2.1 Local Historical Storage (`app/market/historical/storage.py`)
* **`HistoricalBarRepository` (ABC)**: Abstract interface defining contracts for storing, querying, counting, summarizing, and deleting historical bars.
* **`SQLiteHistoricalBarRepository`**: Production-grade local SQLite persistence layer.
  * **Dual Inheritance**: Implements both `HistoricalBarRepository` and `HistoricalDataProvider`, enabling direct plugging into loaders and replay engines.
  * **Schema Definition**: Table `historical_bars` with schema versioning (`schema_meta` table), storing `symbol`, `exchange`, `timeframe`, `timestamp` (POSIX UTC float), `open`, `high`, `low`, `close`, `volume`, `source`, `is_zero_volume`, `quality_status`, and `created_at`.
  * **Uniqueness Invariant**: `UNIQUE(symbol, timeframe, timestamp)` prevents duplicate bars.
  * **Conflict Resolution**: Configurable `on_conflict="replace"` (updates existing records) or `"ignore"` (skips duplicate insertion).
  * **Transactional Concurrency**: Uses SQLite `WAL` mode, `PRAGMA synchronous=NORMAL`, `PRAGMA busy_timeout=5000`, and thread-safe reentrant locking (`threading.RLock`).
  * **Corruption Detection**: Executes `PRAGMA integrity_check;` via `verify_integrity()`. Detects invalid or corrupted bar values during queries and raises `ValidationError` explicitly without silent swallowing.

### 2.2 Historical Data Cache (`app/market/historical/cache.py`)
* **`HistoricalDataCache`**: Thread-safe bounded in-memory cache utilizing `collections.OrderedDict` for LRU eviction.
* **Deterministic Keys**: `make_cache_key(symbol, timeframe, start_time, end_time, limit)` generates normalized, rounded tuples ensuring identical queries hit the same entry.
* **Bounded Capacity**: Bounded by `max_size` (default 200). Least recently used items are evicted upon reaching capacity.
* **TTL Expiration**: Configurable time-to-live (`ttl_seconds`, default 300.0s). Expired entries are evicted on read.
* **Stream-Level Invalidation**: `invalidate_stream(symbol, timeframe=None)` immediately flushes cache entries for a stream whenever new bars are written to the repository.
* **Partial / Failure Rejection**: Incomplete or failed queries are never cached (`is_complete=True` check).
* **Observable Telemetry**: Tracks `hits`, `misses`, `evictions`, `invalidations`, and calculates `hit_rate`.

### 2.3 Deterministic Replay Engine (`app/market/historical/replay.py`)
* **`HistoricalReplayEngine`**: Orchestrates chronological playback of stored historical bars through the quality gate and technical analysis engine.
* **Strict Chronological Playback**: Queries bars ordered by `timestamp ASC` and replays them sequentially.
* **Analysis State Isolation**: If `reset_analysis_state=True` (default), resets the `RollingBarSeries` and cached feature vectors in `TechnicalAnalysisEngine` and resets stream tracking in `MarketDataQualityEngine`, ensuring total state isolation between runs.
* **Configurable Pacing**: Supports `pacing_seconds=0.0` for sub-second offline testing and positive values for paced emulation.
* **Quality Gate Integration**: Each bar is evaluated via `MarketDataQualityEngine.evaluate_bar` with `reference_time=bar.timestamp`. Corrupt or out-of-order bars are quarantined and never enter rolling analysis windows.
* **Replay Data Labeling**: Stamped with `source="replay"`, strictly isolating replay data from live market quotes.
* **Cooperative Cancellation**: Supports `cancel_replay(replay_id)` and `threading.Event` cancellation tokens.
* **Lifecycle Observability**: Publishes `HISTORICAL_REPLAY_STARTED`, `HISTORICAL_REPLAY_PROGRESS`, `HISTORICAL_REPLAY_COMPLETED`, `HISTORICAL_REPLAY_CANCELLED`, and `HISTORICAL_REPLAY_FAILED` events. Emits state updates to `StateStore`.

### 2.4 Orchestrator Commands (`app/market/historical/handlers.py`)
* `store_historical_bars`: Persists a collection of bars or loads and stores bars from provider.
* `query_historical_bars`: Bounded query of stored bars from SQLite repository.
* `replay_historical_bars`: Initiates deterministic historical replay and returns structured summary metrics.

### 2.5 Analysis & Quality Engine Extensions
* **`TechnicalAnalysisEngine.reset_stream(symbol, timeframe)`**: Thread-safe method to clear existing bar series, cached feature vectors, and quarantine tracking for a specific stream.
* **`MarketDataQualityEngine.reset_stream(symbol, source, data_type)`**: Thread-safe method to clear timestamp and fingerprint tracking for an isolated replay stream.

### 2.6 System Bootstrap Integration (`app/bootstrap.py`)
* Initialized `SQLiteHistoricalBarRepository` (`data/kaiser.db`), `HistoricalDataCache`, `HistoricalDataLoader`, and `HistoricalReplayEngine`.
* Wired all components into `SystemStatus` and registered all command handlers with `MasterOrchestrator`.

---

## 3. Files Created and Modified

| File | Status | Description |
|---|---|---|
| `app/market/historical/storage.py` | **Created** | `HistoricalBarRepository` ABC and `SQLiteHistoricalBarRepository` with WAL mode and uniqueness constraints. |
| `app/market/historical/cache.py` | **Created** | `HistoricalDataCache` bounded LRU cache with TTL, invalidation, and metrics. |
| `app/market/historical/replay.py` | **Created** | `HistoricalReplayEngine`, requests, results, chronological playback, and cancellation. |
| `tests/test_historical_storage_and_replay.py` | **Created** | 28 deterministic unit tests for storage, cache, replay, commands, and safety. |
| `app/market/historical/loader.py` | **Modified** | Added repository and cache injection, cache lookup on load, and cache invalidation on store. |
| `app/market/historical/handlers.py` | **Modified** | Registered `store_historical_bars`, `query_historical_bars`, `replay_historical_bars` on orchestrator. |
| `app/market/historical/__init__.py` | **Modified** | Re-exported repository, cache, and replay engine classes and helpers. |
| `app/market/__init__.py` | **Modified** | Package re-exports for historical persistence and replay components. |
| `app/analysis/engine.py` | **Modified** | Added `reset_stream(symbol, timeframe)` for state isolation ahead of replays. |
| `app/market/quality/engine.py` | **Modified** | Added `reset_stream(...)` to reset stream-level tracking during replay runs. |
| `app/core/events/event.py` | **Modified** | Added historical storage, query, and replay lifecycle event types. |
| `app/bootstrap.py` | **Modified** | Bootstrapped repository, cache, and replay engine, wiring them into `SystemStatus`. |
| `.gitignore` | **Modified** | Added `*.db`, `*.sqlite`, `*.sqlite3` to prevent database persistence in git. |

---

## 4. Verification and Test Results

### 4.1 Focused Test Execution
```
python -m pytest tests/test_historical_storage_and_replay.py
============================= 28 passed in 0.93s ==============================

python -m pytest tests/test_historical_data.py
============================= 28 passed in 0.66s ==============================
```

### 4.2 Full Repository Test Suite
```
python -m pytest
============================= 515 passed in 7.77s =============================
```
* **515 passing tests** across 16 test files.
* **0 errors, 0 failures, 0 regressions.**

### 4.3 Runtime Application Startup (`python run.py`)
```
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Bootstrapping KAISER foundation layer...
...
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Historical data loader: ready (provider=mock_historical_provider)
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Historical repository: ready (db=data/kaiser.db)
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Historical cache: ready (max_size=200)
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Historical replay engine: ready
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Trading engine: not initialized
2026-10-03 01:16:36 | INFO     | kaiser.bootstrap | Execution engine: locked
KAISER
System initialized successfully.
Environment: development
Voice-first architecture: ready
Trading engine: not initialized
Execution engine: locked
```

---

## 5. Architectural Invariants and Safety Guarantees

1. **Trading Execution Lock**: `Settings.execution_locked = True` is strictly maintained. Trading engine is `not initialized`, execution engine is `locked`.
2. **Offline Startup**: Startup does not initiate automatic network downloads, database fills, or replay tasks.
3. **No Strategy Profitability Claims**: Replay is strictly data playback and indicator computation infrastructure; no backtesting or PnL assertions are made.
4. **Data Isolation**: Replayed bars are strictly marked `source="replay"`, preventing confusion with live market quotes.
5. **No Secret Storage**: Database stores only market bar records; no tokens, credentials, or audio captures are persisted.
6. **SQL Injection Prevention**: All queries and insertions use parameterized queries (`?`).
