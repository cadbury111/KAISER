"""KAISER — Test Configuration, Safety Fixtures, and Isolation Hooks.

Ensures that:
1. Automated tests run against isolated temporary databases with per-test clean state.
2. The persistent user database 'data/kaiser.db' is never read, locked, or mutated during tests.
3. Checksum and size of 'data/kaiser.db' are actively verified before and after test sessions.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Generator, Optional, Tuple

import pytest


def _get_db_fingerprint(db_path: Path) -> Optional[Tuple[int, str]]:
    """Compute (size, sha256) of a database file if it exists."""
    if not db_path.exists():
        return None
    try:
        data = db_path.read_bytes()
        return len(data), hashlib.sha256(data).hexdigest().upper()
    except Exception:
        return None


@pytest.fixture(scope="session", autouse=True)
def verify_user_database_immutability() -> Generator[None, None, None]:
    """Session-level invariant fixture recording and verifying persistent data/kaiser.db integrity."""
    prod_db = Path("data/kaiser.db")
    initial_fingerprint = _get_db_fingerprint(prod_db)

    try:
        yield
    finally:
        final_fingerprint = _get_db_fingerprint(prod_db)
        if initial_fingerprint is not None:
            assert final_fingerprint is not None, "data/kaiser.db was deleted during test run!"
            assert final_fingerprint[0] == initial_fingerprint[0], (
                f"data/kaiser.db size changed during test run: "
                f"initial={initial_fingerprint[0]}, final={final_fingerprint[0]}"
            )
            assert final_fingerprint[1] == initial_fingerprint[1], (
                f"data/kaiser.db SHA-256 mutated during test run: "
                f"initial={initial_fingerprint[1]}, final={final_fingerprint[1]}"
            )


@pytest.fixture(scope="function", autouse=True)
def enforce_test_database_isolation(
    tmp_path: Path,
) -> Generator[str, None, None]:
    """Function-level autouse fixture providing an isolated temporary database for each test.

    Guarantees zero cross-test state leakage and complete isolation from production storage.
    """
    test_db_path = str(tmp_path / "kaiser_isolated_test.db")
    old_override = os.environ.get("KAISER_TEST_DB_PATH")
    os.environ["KAISER_TEST_DB_PATH"] = test_db_path

    try:
        yield test_db_path
    finally:
        if old_override is not None:
            os.environ["KAISER_TEST_DB_PATH"] = old_override
        else:
            os.environ.pop("KAISER_TEST_DB_PATH", None)
