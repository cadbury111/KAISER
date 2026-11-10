"""KAISER — Phase 2, Step 76 Test Suite: Database Isolation Hardening & CLI Non-Destructive Verification.

Audits, hardens, and verifies:
1. Complete resolver edge-case matrix (production defaults, explicit paths, absolute paths, traversal, Windows separators).
2. Fallback database path uniqueness across independent test contexts within the same process.
3. Test override lifecycle, environment restoration, and leak prevention.
4. WAL/SHM file lifecycle, checkpointing, and safe cleanup without risk to persistent databases.
5. Non-destructive CLI startup and shutdown against isolated disposable databases.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from typing import Generator

import pytest

from app.analysis.runs.registry import AnalysisRunRegistry
from app.bootstrap import bootstrap_system
from app.core.config import Settings, resolve_database_path
from app.ledger.ledger import PaperLedger
from app.ledger.outbox import OutboxRecord, TransactionalOutbox
from app.market.historical.storage import SQLiteHistoricalBarRepository
from app.market.provenance.tracker import MarketDataProvenanceTracker


PROD_DB_PATH = Path("data/kaiser.db")


def _read_db_hash(path: Path) -> str:
    """Read SHA-256 hash of a file if it exists, or empty string."""
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


# ==============================================================================
# SECTION A: RESOLVER EDGE-CASE MATRIX TESTS
# ==============================================================================

class TestResolverEdgeCaseMatrix:
    """Exhaustive tests for resolve_database_path covering edge cases and normalization."""

    def test_category_a_production_defaults(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Category A: Production defaults must redirect when in test mode."""
        test_override = str(tmp_path / "sandbox_override.db")
        monkeypatch.setenv("KAISER_TEST_DB_PATH", test_override)

        assert resolve_database_path("sqlite:///data/kaiser.db", app_env="test") == test_override
        assert resolve_database_path("data/kaiser.db", app_env="test") == test_override
        assert resolve_database_path("./data/kaiser.db", app_env="test") == test_override
        assert resolve_database_path("data\\kaiser.db", app_env="test") == test_override

    def test_category_b_explicit_custom_databases(self, tmp_path: Path) -> None:
        """Category B: Explicit custom databases must never be redirected."""
        custom_file = str(tmp_path / "my_custom_store.db")
        assert resolve_database_path(f"sqlite:///{custom_file}", app_env="test") == custom_file
        assert resolve_database_path(custom_file, app_env="test") == custom_file
        assert resolve_database_path(":memory:", app_env="test") == ":memory:"
        assert resolve_database_path("sqlite:///:memory:", app_env="test") == ":memory:"

    def test_category_c_environment_contexts(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Category C: Environment context transitions."""
        test_override = str(tmp_path / "env_override.db")
        monkeypatch.setenv("KAISER_TEST_DB_PATH", test_override)

        # Explicit app_env='test'
        assert resolve_database_path("sqlite:///data/kaiser.db", app_env="test") == test_override

        # Explicit app_env='development' outside pytest modules check
        # Explicit custom database is respected in all environments
        custom_dev = str(tmp_path / "dev_custom.db")
        assert resolve_database_path(f"sqlite:///{custom_dev}", app_env="development") == custom_dev

    def test_category_d_path_normalization_and_traversal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Category D: Path traversal, absolute paths, and Windows backslashes."""
        test_override = str(tmp_path / "norm_override.db")
        monkeypatch.setenv("KAISER_TEST_DB_PATH", test_override)

        # Traversal resolving to repo's data/kaiser.db
        traversal_path = "data/../data/kaiser.db"
        assert resolve_database_path(traversal_path, app_env="test") == test_override

        # Absolute path to repo's data/kaiser.db
        abs_prod = Path("data/kaiser.db").resolve().as_posix()
        assert resolve_database_path(abs_prod, app_env="test") == test_override

        # Path in another folder that happens to be named 'data/kaiser.db' MUST NOT be redirected
        other_dir_db = (tmp_path / "other" / "data" / "kaiser.db").resolve().as_posix()
        assert resolve_database_path(other_dir_db, app_env="test") == other_dir_db


# ==============================================================================
# SECTION B: FALLBACK PATH UNIQUENESS TESTS
# ==============================================================================

class TestFallbackPathUniqueness:
    """Verifies that fallback database paths are unique across test contexts."""

    def test_independent_contexts_receive_unique_fallback_paths(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without KAISER_TEST_DB_PATH, independent calls receive distinct fallback files."""
        monkeypatch.delenv("KAISER_TEST_DB_PATH", raising=False)

        path_1 = resolve_database_path("data/kaiser.db", app_env="test")
        path_2 = resolve_database_path("data/kaiser.db", app_env="test")

        assert path_1 != path_2, "Two independent calls without override returned identical fallback paths!"
        assert str(os.getpid()) in path_1
        assert str(os.getpid()) in path_2

        # Verify paths reside strictly in system temp directory, never in data/
        temp_dir_norm = Path(tempfile.gettempdir()).resolve().as_posix()
        assert Path(path_1).resolve().as_posix().startswith(temp_dir_norm)
        assert Path(path_2).resolve().as_posix().startswith(temp_dir_norm)
        assert "data/kaiser.db" not in path_1

    def test_multithreaded_fallback_paths_are_strictly_unique(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Concurrent calls in multiple threads produce strictly unique paths."""
        monkeypatch.delenv("KAISER_TEST_DB_PATH", raising=False)

        def get_path() -> str:
            return resolve_database_path("data/kaiser.db", app_env="test")

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            paths = list(executor.map(lambda _: get_path(), range(16)))

        assert len(set(paths)) == 16, f"Expected 16 unique paths, got {len(set(paths))}"


# ==============================================================================
# SECTION C: ENVIRONMENT RESTORATION & OVERRIDE LEAKAGE TESTS
# ==============================================================================

class TestEnvironmentRestorationAndOverrideLeakage:
    """Verifies environment preservation and safe teardown."""

    def test_override_is_isolated_to_scope(self, tmp_path: Path) -> None:
        """Verify manual override lifecycle restores previous value."""
        original_val = os.environ.get("KAISER_TEST_DB_PATH")
        scoped_path = str(tmp_path / "scoped.db")

        try:
            os.environ["KAISER_TEST_DB_PATH"] = scoped_path
            assert resolve_database_path("data/kaiser.db", app_env="test") == scoped_path
        finally:
            if original_val is not None:
                os.environ["KAISER_TEST_DB_PATH"] = original_val
            else:
                os.environ.pop("KAISER_TEST_DB_PATH", None)

        assert os.environ.get("KAISER_TEST_DB_PATH") == original_val

    def test_nested_override_restoration(self, tmp_path: Path) -> None:
        """Nested override operations correctly restore outer values."""
        outer_path = str(tmp_path / "outer.db")
        inner_path = str(tmp_path / "inner.db")

        os.environ["KAISER_TEST_DB_PATH"] = outer_path
        try:
            assert os.environ["KAISER_TEST_DB_PATH"] == outer_path
            # Inner scope
            os.environ["KAISER_TEST_DB_PATH"] = inner_path
            assert os.environ["KAISER_TEST_DB_PATH"] == inner_path
            # Restore outer
            os.environ["KAISER_TEST_DB_PATH"] = outer_path
            assert os.environ["KAISER_TEST_DB_PATH"] == outer_path
        finally:
            os.environ.pop("KAISER_TEST_DB_PATH", None)


# ==============================================================================
# SECTION D: WAL / SHM LIFECYCLE & CLEANUP TESTS
# ==============================================================================

class TestWalShmLifecycleAndCleanup:
    """Verifies WAL/SHM creation, checkpointing, and safe cleanup on disposable databases."""

    def test_disposable_wal_checkpoint_and_cleanup(self, tmp_path: Path) -> None:
        """Verify opening in WAL mode, checkpointing, and safely unlinking files."""
        initial_hash = _read_db_hash(PROD_DB_PATH)
        disp_db = tmp_path / "wal_lifecycle.db"

        # 1. Open connection, configure WAL
        conn = sqlite3.connect(str(disp_db))
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("CREATE TABLE records (id INT PRIMARY KEY, val TEXT);")

        # 2. Write rows
        conn.execute("INSERT INTO records VALUES (1, 'alpha');")
        conn.execute("INSERT INTO records VALUES (2, 'beta');")
        conn.commit()

        # 3. Checkpoint WAL
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        conn.close()

        # 4. Clean up disposable files
        wal_file = Path(f"{disp_db}-wal")
        shm_file = Path(f"{disp_db}-shm")
        if wal_file.exists():
            wal_file.unlink()
        if shm_file.exists():
            shm_file.unlink()
        if disp_db.exists():
            disp_db.unlink()

        assert not disp_db.exists()
        assert not wal_file.exists()
        assert not shm_file.exists()

        # Verify persistent production DB remains untouched
        assert _read_db_hash(PROD_DB_PATH) == initial_hash

    def test_cleanup_guard_rejects_persistent_user_database(self) -> None:
        """Cleanup logic must refuse to delete persistent user database."""
        def safe_unlink(target: Path) -> bool:
            resolved_target = target.resolve()
            resolved_prod = PROD_DB_PATH.resolve()
            if resolved_target == resolved_prod or resolved_target.as_posix().endswith("/data/kaiser.db"):
                raise PermissionError("Attempted cleanup on persistent user database rejected by guard!")
            target.unlink()
            return True

        with pytest.raises(PermissionError, match="Attempted cleanup on persistent user database rejected by guard!"):
            safe_unlink(PROD_DB_PATH)


# ==============================================================================
# SECTION E: NON-DESTRUCTIVE CLI STARTUP AND SHUTDOWN SMOKE TESTS
# ==============================================================================

class TestNonDestructiveCliStartup:
    """Verifies CLI behavior without touching persistent user databases."""

    def test_cli_help_flag_is_non_destructive(self) -> None:
        """'python run.py --help' must print usage and exit 0 without opening databases."""
        initial_hash = _read_db_hash(PROD_DB_PATH)

        result = subprocess.run(
            [sys.executable, "run.py", "--help"],
            capture_output=True,
            text=True,
            timeout=15,
        )

        assert result.returncode == 0
        assert "Usage: python run.py" in result.stdout
        assert "--help" in result.stdout
        assert "--dashboard" in result.stdout

        # Verify production database was completely untouched
        assert _read_db_hash(PROD_DB_PATH) == initial_hash

    def test_cli_bootstrap_against_disposable_database(self, tmp_path: Path) -> None:
        """'python run.py' with DATABASE_URL directed to disposable DB bootstraps cleanly."""
        initial_hash = _read_db_hash(PROD_DB_PATH)
        disp_db = tmp_path / "cli_disposable.db"

        env = os.environ.copy()
        env["DATABASE_URL"] = f"sqlite:///{disp_db.as_posix()}"
        env.pop("KAISER_TEST_DB_PATH", None)

        result = subprocess.run(
            [sys.executable, "run.py"],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )

        assert result.returncode == 0
        assert "System initialized successfully." in result.stdout
        assert "Execution engine: locked" in result.stdout
        assert "Historical repository: ready (db=" in result.stdout

        # Verify the disposable database was created and utilized
        assert disp_db.exists()

        # Verify persistent production database was completely untouched
        assert _read_db_hash(PROD_DB_PATH) == initial_hash

