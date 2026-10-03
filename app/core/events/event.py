"""KAISER — Immutable Event System.

Defines the core immutable Event data structure, standard event types,
and immutability helpers for auditability and subsystem decoupling.
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


class EventType(str, Enum):
    """Standardized KAISER event types across the trading intelligence lifecycle."""

    # System Lifecycle
    SYSTEM_INITIALIZED = "SYSTEM_INITIALIZED"
    SYSTEM_SHUTDOWN = "SYSTEM_SHUTDOWN"
    SYSTEM_HEALTH_CHECK = "SYSTEM_HEALTH_CHECK"

    # Voice & Interaction
    VOICE_COMMAND_RECEIVED = "VOICE_COMMAND_RECEIVED"
    COMMAND_PARSED = "COMMAND_PARSED"
    VOICE_INPUT_RECEIVED = "VOICE_INPUT_RECEIVED"
    VOICE_TRANSCRIPTION_STARTED = "VOICE_TRANSCRIPTION_STARTED"
    VOICE_TRANSCRIPTION_COMPLETED = "VOICE_TRANSCRIPTION_COMPLETED"
    VOICE_TRANSCRIPTION_EMPTY = "VOICE_TRANSCRIPTION_EMPTY"
    VOICE_TRANSCRIPTION_FAILED = "VOICE_TRANSCRIPTION_FAILED"
    VOICE_SESSION_CREATED = "VOICE_SESSION_CREATED"
    VOICE_SESSION_UPDATED = "VOICE_SESSION_UPDATED"
    VOICE_SESSION_CLOSED = "VOICE_SESSION_CLOSED"
    VOICE_RESPONSE_CREATED = "VOICE_RESPONSE_CREATED"
    VOICE_PROVIDER_ERROR = "VOICE_PROVIDER_ERROR"

    # Voice Turn Lifecycle (Step 09 & 11)
    VOICE_TURN_STARTED = "VOICE_TURN_STARTED"
    VOICE_TURN_COMPLETED = "VOICE_TURN_COMPLETED"
    VOICE_TURN_FAILED = "VOICE_TURN_FAILED"
    VOICE_TURN_CANCELLED = "VOICE_TURN_CANCELLED"
    VOICE_TURN_REJECTED = "VOICE_TURN_REJECTED"

    # Voice Synthesis & Audio Playback (Step 10 & 11)
    VOICE_SYNTHESIS_STARTED = "VOICE_SYNTHESIS_STARTED"
    VOICE_SYNTHESIS_COMPLETED = "VOICE_SYNTHESIS_COMPLETED"
    VOICE_SYNTHESIS_EMPTY = "VOICE_SYNTHESIS_EMPTY"
    VOICE_SYNTHESIS_FAILED = "VOICE_SYNTHESIS_FAILED"
    VOICE_SYNTHESIS_UNSUPPORTED = "VOICE_SYNTHESIS_UNSUPPORTED"
    AUDIO_PLAYBACK_STARTED = "AUDIO_PLAYBACK_STARTED"
    AUDIO_PLAYBACK_COMPLETED = "AUDIO_PLAYBACK_COMPLETED"
    AUDIO_PLAYBACK_CANCELLED = "AUDIO_PLAYBACK_CANCELLED"
    AUDIO_PLAYBACK_FAILED = "AUDIO_PLAYBACK_FAILED"

    # Audio Capture & Devices (Step 07)
    AUDIO_DEVICE_DISCOVERED = "AUDIO_DEVICE_DISCOVERED"
    AUDIO_CAPTURE_STARTED = "AUDIO_CAPTURE_STARTED"
    AUDIO_CAPTURE_COMPLETED = "AUDIO_CAPTURE_COMPLETED"
    AUDIO_CAPTURE_CANCELLED = "AUDIO_CAPTURE_CANCELLED"
    AUDIO_CAPTURE_FAILED = "AUDIO_CAPTURE_FAILED"

    # Request Lifecycle & Orchestration
    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    REQUEST_STARTED = "REQUEST_STARTED"
    REQUEST_COMPLETED = "REQUEST_COMPLETED"
    REQUEST_FAILED = "REQUEST_FAILED"
    REQUEST_CANCELLED = "REQUEST_CANCELLED"

    # Market Data
    MARKET_QUOTE_UPDATED = "MARKET_QUOTE_UPDATED"
    MARKET_BAR_UPDATED = "MARKET_BAR_UPDATED"
    MARKET_DEPTH_UPDATED = "MARKET_DEPTH_UPDATED"
    MARKET_DATA_ERROR = "MARKET_DATA_ERROR"
    MARKET_DATA_STALE = "MARKET_DATA_STALE"

    # Historical Market Data (Step 03 & Step 04)
    HISTORICAL_DATA_REQUESTED = "HISTORICAL_DATA_REQUESTED"
    HISTORICAL_DATA_LOADED = "HISTORICAL_DATA_LOADED"
    HISTORICAL_DATA_INGESTED = "HISTORICAL_DATA_INGESTED"
    HISTORICAL_DATA_FAILED = "HISTORICAL_DATA_FAILED"
    HISTORICAL_DATA_STORED = "HISTORICAL_DATA_STORED"
    HISTORICAL_DATA_QUERIED = "HISTORICAL_DATA_QUERIED"
    HISTORICAL_REPLAY_STARTED = "HISTORICAL_REPLAY_STARTED"
    HISTORICAL_REPLAY_PROGRESS = "HISTORICAL_REPLAY_PROGRESS"
    HISTORICAL_REPLAY_COMPLETED = "HISTORICAL_REPLAY_COMPLETED"
    HISTORICAL_REPLAY_CANCELLED = "HISTORICAL_REPLAY_CANCELLED"
    HISTORICAL_REPLAY_FAILED = "HISTORICAL_REPLAY_FAILED"

    # Market Data Quality & Reliability
    MARKET_DATA_QUALITY_UPDATED = "MARKET_DATA_QUALITY_UPDATED"
    MARKET_DATA_DEGRADED = "MARKET_DATA_DEGRADED"
    MARKET_DATA_INVALID = "MARKET_DATA_INVALID"
    MARKET_DATA_MISSING = "MARKET_DATA_MISSING"
    MARKET_DATA_DUPLICATE = "MARKET_DATA_DUPLICATE"
    MARKET_DATA_OUT_OF_ORDER = "MARKET_DATA_OUT_OF_ORDER"
    MARKET_SOURCE_HEALTH_UPDATED = "MARKET_SOURCE_HEALTH_UPDATED"

    # Analysis & Specialist Agents
    ANALYSIS_REQUESTED = "ANALYSIS_REQUESTED"
    ANALYSIS_INDICATOR_CALCULATED = "ANALYSIS_INDICATOR_CALCULATED"
    ANALYSIS_FEATURE_VECTOR_READY = "ANALYSIS_FEATURE_VECTOR_READY"
    ANALYSIS_MULTI_TIMEFRAME_READY = "ANALYSIS_MULTI_TIMEFRAME_READY"
    AGENT_RESPONSE = "AGENT_RESPONSE"

    # Risk Management Gate
    RISK_ASSESSMENT_REQUESTED = "RISK_ASSESSMENT_REQUESTED"
    RISK_APPROVAL_REQUIRED = "RISK_APPROVAL_REQUIRED"
    RISK_APPROVED = "RISK_APPROVED"
    RISK_REJECTED = "RISK_REJECTED"

    # Trade Proposal & Human Approval Gate
    TRADE_PROPOSAL_CREATED = "TRADE_PROPOSAL_CREATED"
    USER_APPROVAL_REQUIRED = "USER_APPROVAL_REQUIRED"
    USER_APPROVED = "USER_APPROVED"
    USER_REJECTED = "USER_REJECTED"

    # Execution & Portfolio (Foundation Invariant: Locked by default)
    EXECUTION_REQUESTED = "EXECUTION_REQUESTED"
    EXECUTION_RESULT = "EXECUTION_RESULT"
    PORTFOLIO_UPDATED = "PORTFOLIO_UPDATED"
    LEDGER_ENTRY_RECORDED = "LEDGER_ENTRY_RECORDED"

    # Backtesting & Strategy Evaluation (Phase 2, Step 05)
    BACKTEST_STARTED = "BACKTEST_STARTED"
    BACKTEST_PROGRESS = "BACKTEST_PROGRESS"
    BACKTEST_COMPLETED = "BACKTEST_COMPLETED"
    BACKTEST_CANCELLED = "BACKTEST_CANCELLED"
    BACKTEST_FAILED = "BACKTEST_FAILED"

    # Walk-Forward Validation & Out-of-Sample Evaluation (Phase 2, Step 06)
    WALK_FORWARD_STARTED = "WALK_FORWARD_STARTED"
    WALK_FORWARD_PROGRESS = "WALK_FORWARD_PROGRESS"
    WALK_FORWARD_COMPLETED = "WALK_FORWARD_COMPLETED"
    WALK_FORWARD_CANCELLED = "WALK_FORWARD_CANCELLED"
    WALK_FORWARD_FAILED = "WALK_FORWARD_FAILED"

    # Strategy Robustness & Stress Testing (Phase 2, Step 07)
    ROBUSTNESS_TEST_STARTED = "ROBUSTNESS_TEST_STARTED"
    ROBUSTNESS_TEST_PROGRESS = "ROBUSTNESS_TEST_PROGRESS"
    ROBUSTNESS_TEST_COMPLETED = "ROBUSTNESS_TEST_COMPLETED"
    ROBUSTNESS_TEST_CANCELLED = "ROBUSTNESS_TEST_CANCELLED"
    ROBUSTNESS_TEST_FAILED = "ROBUSTNESS_TEST_FAILED"

    # Portfolio Simulation & Multi-Strategy Analytics (Phase 2, Step 08)
    PORTFOLIO_SIMULATION_STARTED = "PORTFOLIO_SIMULATION_STARTED"
    PORTFOLIO_SIMULATION_PROGRESS = "PORTFOLIO_SIMULATION_PROGRESS"
    PORTFOLIO_SIMULATION_COMPLETED = "PORTFOLIO_SIMULATION_COMPLETED"
    PORTFOLIO_SIMULATION_CANCELLED = "PORTFOLIO_SIMULATION_CANCELLED"
    PORTFOLIO_SIMULATION_FAILED = "PORTFOLIO_SIMULATION_FAILED"



def _freeze_container(data: Any) -> Any:
    """Recursively freeze dictionaries and lists into immutable containers."""
    if isinstance(data, dict):
        return MappingProxyType({k: _freeze_container(v) for k, v in data.items()})
    if isinstance(data, (list, tuple)):
        return tuple(_freeze_container(item) for item in data)
    if isinstance(data, set):
        return frozenset(_freeze_container(item) for item in data)
    return data


@dataclass(frozen=True)
class Event:
    """An immutable event representing a fact that occurred in KAISER.

    Attributes:
        event_id: Unique identifier for this specific event.
        event_type: Type of the event (EventType enum or custom string).
        timestamp: POSIX UTC timestamp when the event was generated.
        source: Subsystem that originated the event (e.g. 'voice', 'risk').
        correlation_id: Traceability identifier threading related events.
        payload: Immutable key-value payload carrying event data.
        metadata: Immutable metadata carrying auxiliary routing/context data.
    """

    event_id: str
    event_type: str
    timestamp: float
    source: str
    correlation_id: str
    payload: Mapping[str, Any]
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        """Enforce strict immutability and validate mandatory fields."""
        if not self.event_id:
            raise ValueError("event_id must not be empty.")
        if not self.event_type:
            raise ValueError("event_type must not be empty.")
        if not self.source:
            raise ValueError("source must not be empty.")
        if not self.correlation_id:
            raise ValueError("correlation_id must not be empty.")

        # Ensure event_type is stored as clean string
        normalized_type = (
            self.event_type.value
            if isinstance(self.event_type, Enum)
            else str(self.event_type)
        )
        object.__setattr__(self, "event_type", normalized_type)

        # Freeze payload and metadata recursively
        object.__setattr__(self, "payload", _freeze_container(self.payload or {}))
        object.__setattr__(self, "metadata", _freeze_container(self.metadata or {}))

    @classmethod
    def create(
        cls,
        event_type: EventType | str,
        source: str,
        payload: Mapping[str, Any] | None = None,
        correlation_id: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        event_id: str | None = None,
        timestamp: float | None = None,
    ) -> Event:
        """Factory method to construct an Event with safe defaults.

        Args:
            event_type: The EventType or string type descriptor.
            source: Name of the originating subsystem.
            payload: Optional dictionary carrying event data.
            correlation_id: Tracing ID. If omitted, a new UUID4 is generated.
            metadata: Optional auxiliary metadata.
            event_id: Specific event ID. If omitted, a new UUID4 is generated.
            timestamp: Specific POSIX timestamp. If omitted, time.time() is used.

        Returns:
            Event: The initialized immutable event.
        """
        resolved_event_id = event_id or uuid.uuid4().hex
        resolved_correlation_id = correlation_id or uuid.uuid4().hex
        resolved_timestamp = time.time() if timestamp is None else timestamp
        clean_payload = copy.deepcopy(dict(payload or {}))
        clean_metadata = copy.deepcopy(dict(metadata or {}))

        return cls(
            event_id=resolved_event_id,
            event_type=event_type,
            timestamp=resolved_timestamp,
            source=source,
            correlation_id=resolved_correlation_id,
            payload=clean_payload,
            metadata=clean_metadata,
        )

    @property
    def iso_timestamp(self) -> str:
        """Return the event timestamp formatted as ISO-8601 UTC string."""
        dt = datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
        return dt.isoformat()

    def to_dict(self) -> dict[str, Any]:
        """Return a mutable serializable dictionary representation of the event."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "source": self.source,
            "correlation_id": self.correlation_id,
            "payload": copy.deepcopy(dict(self.payload)),
            "metadata": copy.deepcopy(dict(self.metadata)),
        }
