"""KAISER — Thread-Safe State Store.

Provides a centralized, isolated, thread-safe in-memory state repository.
Enforces mutation protection through deep-copy isolation and guards against
accidental credential storage.
"""

from __future__ import annotations

import copy
import re
import threading
from typing import Any, Callable, Dict, List, Optional

from app.core.exceptions import SecurityError
from app.core.logging_config import get_logger

logger = get_logger("state.store")

# Prohibited key patterns that indicate attempts to store raw secrets in state
PROHIBITED_KEY_PATTERNS = (
    re.compile(r".*(api[_-]?key|secret|password|private[_-]?key|auth[_-]?token).*", re.IGNORECASE),
)


class StateStore:
    """Thread-safe centralized state repository for KAISER.

    Maintains current application state across subsystems (quotes, voice session,
    risk evaluation, proposals, portfolio). Ensures:
    1. Full thread safety via reentrant locks (RLock).
    2. Zero external mutation side-effects via deep copying.
    3. Strict safety guardrails forbidding credential persistence.
    """

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self._lock = threading.RLock()
        self._state: Dict[str, Any] = {}
        self._version: int = 0

    @property
    def version(self) -> int:
        """Monotonically increasing version counter incremented on every state mutation."""
        with self._lock:
            return self._version

    def _validate_safety(self, key: str, value: Any) -> None:
        """Verify that state keys do not represent raw credentials or secrets."""
        for pattern in PROHIBITED_KEY_PATTERNS:
            if pattern.match(key):
                raise SecurityError(
                    f"Security violation: Attempted to store sensitive credential in StateStore "
                    f"under key '{key}'. Raw credentials and secrets must never be placed in StateStore."
                )

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a deep-copied value by key to prevent caller mutation of internal state.

        Args:
            key: State entry key.
            default: Value returned if key is absent.

        Returns:
            Any: Deep copy of the stored value, or default.
        """
        with self._lock:
            if key not in self._state:
                return copy.deepcopy(default)
            return copy.deepcopy(self._state[key])

    def set(self, key: str, value: Any) -> None:
        """Store a deep-copied value under the given key.

        Args:
            key: State entry key.
            value: Data to store (will be deep-copied).

        Raises:
            SecurityError: If key matches sensitive credential patterns.
        """
        self._validate_safety(key, value)
        with self._lock:
            self._state[key] = copy.deepcopy(value)
            self._version += 1
            logger.debug("StateStore '%s' updated key '%s' (v%d)", self.name, key, self._version)

    def update(
        self,
        key: str,
        updater: Callable[[Any], Any] | dict[str, Any],
        default: Any = None,
    ) -> Any:
        """Atomically update a state value under lock.

        If updater is a callable, it receives the current value (or default)
        and returns the new value. If updater is a dictionary and the current value
        is a dictionary, performs a dictionary merge.

        Args:
            key: State entry key.
            updater: Callable transforming old value to new, or dict to merge.
            default: Default value if key does not exist yet.

        Returns:
            Any: Deep copy of the newly updated value.
        """
        self._validate_safety(key, updater)
        with self._lock:
            if callable(updater):
                current_value = copy.deepcopy(self._state.get(key, default))
                new_value = updater(current_value)
                self._state[key] = copy.deepcopy(new_value)
            elif isinstance(updater, dict):
                current_raw = self._state.get(key, default)
                if isinstance(current_raw, dict):
                    merged = dict(current_raw)
                    for k, v in updater.items():
                        merged[k] = copy.deepcopy(v)
                    self._state[key] = merged
                else:
                    self._state[key] = copy.deepcopy(updater)
            else:
                self._state[key] = copy.deepcopy(updater)

            self._version += 1
            logger.debug("StateStore '%s' atomically updated key '%s' (v%d)", self.name, key, self._version)
            return copy.deepcopy(self._state[key])

    def delete(self, key: str) -> bool:
        """Remove a key from state.

        Args:
            key: State entry key to remove.

        Returns:
            bool: True if key existed and was deleted, False otherwise.
        """
        with self._lock:
            if key in self._state:
                del self._state[key]
                self._version += 1
                logger.debug("StateStore '%s' deleted key '%s' (v%d)", self.name, key, self._version)
                return True
            return False

    def exists(self, key: str) -> bool:
        """Check whether a key exists in the state store.

        Args:
            key: State entry key.

        Returns:
            bool: True if key is present, False otherwise.
        """
        with self._lock:
            return key in self._state

    def snapshot(self) -> Dict[str, Any]:
        """Return a point-in-time, deep-copied dictionary of all stored state.

        Mutating the returned dictionary or its nested elements will have NO effect
        on internal state.

        Returns:
            Dict[str, Any]: Consistent snapshot of all state entries.
        """
        with self._lock:
            return copy.deepcopy(self._state)

    def keys(self) -> List[str]:
        """Return a list of all keys currently present in the store."""
        with self._lock:
            return list(self._state.keys())

    def clear(self) -> None:
        """Reset the state store completely."""
        with self._lock:
            self._state.clear()
            self._version += 1
            logger.debug("StateStore '%s' cleared (v%d)", self.name, self._version)
