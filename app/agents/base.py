"""KAISER — Base Specialist Agent Interface.

Defines foundational lifecycle, communication, and safety contracts for
autonomous intelligence and specialist trading agents.
"""

from __future__ import annotations

import abc
import threading
from typing import Any, Dict, Optional

from app.core.events import EventBus
from app.core.logging_config import get_logger
from app.core.state import StateStore


class BaseAgent(abc.ABC):
    """Abstract base class for KAISER specialist intelligence agents.

    Key Architectural Invariants:
    1. Zero direct broker connectivity or trade execution pathways.
    2. EventBus and StateStore lifecycle integration.
    3. Thread-safe operations protected by internal reentrant locks.
    4. Deterministic, evidence-based reasoning without unsupported certainty.
    5. Clear separation between calculated facts and interpretations.
    """

    def __init__(
        self,
        name: str,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
    ) -> None:
        self.name = name
        self.event_bus = event_bus
        self.state_store = state_store
        self._lock = threading.RLock()
        self.logger = get_logger(f"agent.{name}")

    @property
    @abc.abstractmethod
    def agent_type(self) -> str:
        """Return the category descriptor of the specialist agent."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_status(self) -> Dict[str, Any]:
        """Return operational telemetry and readiness status."""
        raise NotImplementedError
