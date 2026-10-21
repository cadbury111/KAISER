"""KAISER — Controlled Paper Trading Workflow & Simulation Orchestration.

Coordinates explicitly requested paper orders across market data resolution,
pre-trade risk validation, deterministic execution simulation, portfolio accounting,
and auditable ledger reconciliation.

SAFETY INVARIANTS:
1. PURELY SIMULATED: Never connects to live broker, exchange, or order endpoint.
2. EXECUTION LOCKED: Settings.execution_locked remains True; live trading remains uninitialized.
3. FAIL CLOSED: Missing, invalid, or stale data leads to rejection, never synthetic fills.
4. AUDITABLE & RECONCILED: Full correlation ID propagation, ledger verification, and event auditing.
5. NO AUTONOMOUS ROUTING: No strategy-to-order, agent consensus, or voice execution without explicit user request.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import json
import math
from pathlib import Path
import re
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple
import uuid

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.logging_config import get_logger
from app.core.state import StateStore
from app.execution.allocation import PortfolioEquitySnapshot
from app.execution.calibration import (
    HistoricalSpreadCalibrator,
    SpreadCalibrationReport,
    SpreadObservation,
)
from app.execution.engine import PaperExecutionEngine
from app.ledger.outbox import OutboxRecord, TransactionalOutbox
from app.execution.metrics import (
    LatencyStage,
    OrderLifecycleStage,
    OrderLifecycleTracker,
)
from app.execution.models import (
    PaperExecutionResult,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectionCode,
    PaperRiskConfig,
)
from app.execution.portfolio import PaperPortfolio, compute_financial_digest
from app.ledger.ledger import PaperLedger
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.market.models import Quote

logger = get_logger("execution.workflow")

SAFE_SYMBOL_REGEX = re.compile(r"^[A-Za-z0-9_\-\.\:]+$")
DANGEROUS_PAYLOAD_PATTERNS = re.compile(
    r"(<script|eval\(|exec\(|os\.system|subprocess|__import__|import\s+os|system\(|rm\s+-rf)",
    re.IGNORECASE,
)


class WorkflowStage(str, Enum):
    """Execution stages traversed by a paper trading workflow."""

    REQUEST_RECEIVED = "REQUEST_RECEIVED"
    VALIDATION = "VALIDATION"
    MARKET_DATA_RESOLUTION = "MARKET_DATA_RESOLUTION"
    RISK_GATE = "RISK_GATE"
    SIMULATED_EXECUTION = "SIMULATED_EXECUTION"
    STATE_RECONCILIATION = "STATE_RECONCILIATION"
    CANCELLATION = "CANCELLATION"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


class WorkflowStatus(str, Enum):
    """Terminal and intermediate operational states of a paper trading workflow."""

    ACCEPTED = "ACCEPTED"           # Order accepted and pending (e.g. limit order not filled yet)
    FILLED = "FILLED"               # Order executed and filled
    REJECTED = "REJECTED"           # Request or order rejected by validation/risk/market-data
    CANCELLED = "CANCELLED"         # Order cancelled
    FAILED = "FAILED"               # Unhandled execution failure or invariant violation
    RECONCILIATION_FAILED = "RECONCILIATION_FAILED"  # Ledger or state reconciliation discrepancy


@dataclass(frozen=True)
class PaperWorkflowRequest:
    """Explicit, strongly-typed request for a single paper trading workflow execution."""

    symbol: str
    side: PaperOrderSide
    order_type: PaperOrderType
    quantity: float
    limit_price: Optional[float] = None
    client_order_id: Optional[str] = None
    correlation_id: Optional[str] = None
    session_id: Optional[str] = None
    notes: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    quote: Optional[Quote] = None
    sleeve_id: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate input parameters and sanitize against malicious payloads."""
        if not self.symbol or not isinstance(self.symbol, str) or not self.symbol.strip():
            raise ValidationError("PaperWorkflowRequest symbol must be a non-empty string.")
        norm_sym = self.symbol.strip().upper()
        if not SAFE_SYMBOL_REGEX.match(norm_sym):
            raise ValidationError(f"Symbol '{self.symbol}' contains invalid characters.")
        object.__setattr__(self, "symbol", norm_sym)

        if not isinstance(self.side, PaperOrderSide):
            try:
                object.__setattr__(self, "side", PaperOrderSide(str(self.side).upper()))
            except Exception:
                raise ValidationError(f"Invalid PaperOrderSide: {self.side}")

        if not isinstance(self.order_type, PaperOrderType):
            try:
                object.__setattr__(self, "order_type", PaperOrderType(str(self.order_type).upper()))
            except Exception:
                raise ValidationError(f"Invalid PaperOrderType: {self.order_type}")

        if not math.isfinite(self.quantity) or self.quantity <= 0:
            raise ValidationError(f"Quantity must be a positive finite number, got {self.quantity}")

        if self.order_type == PaperOrderType.LIMIT:
            if self.limit_price is None or not math.isfinite(self.limit_price) or self.limit_price <= 0:
                raise ValidationError("Limit orders require a positive finite limit_price.")

        if self.notes:
            if DANGEROUS_PAYLOAD_PATTERNS.search(str(self.notes)):
                raise ValidationError("Notes contain prohibited executable code or shell injection patterns.")

        if self.client_order_id is None:
            object.__setattr__(self, "client_order_id", f"pwf_ord_{uuid.uuid4().hex[:12]}")

        if self.correlation_id is None:
            object.__setattr__(self, "correlation_id", uuid.uuid4().hex)

        if self.sleeve_id is not None:
            clean_sleeve = str(self.sleeve_id).strip()
            if not clean_sleeve:
                raise ValidationError("sleeve_id cannot be an empty string when provided.")
            object.__setattr__(self, "sleeve_id", clean_sleeve)


@dataclass(frozen=True)
class PaperWorkflowOutcome:
    """Immutable outcome of a paper trading workflow with comprehensive audit references."""

    workflow_id: str
    correlation_id: str
    client_order_id: str
    status: WorkflowStatus
    success: bool
    stage: WorkflowStage
    order: Optional[PaperOrder] = None
    fill: Optional[PaperFill] = None
    rejection_code: Optional[str] = None
    rejection_reason: Optional[str] = None
    reconciliation_result: Optional[Dict[str, Any]] = None
    audit_trail: Tuple[str, ...] = field(default_factory=tuple)
    timestamp: float = field(default_factory=time.time)
    session_id: Optional[str] = None
    sleeve_id: Optional[str] = None
    error_message: Optional[str] = None

    @property
    def is_filled(self) -> bool:
        """True if and only if the order reached FILLED status with an executed fill."""
        return (
            self.status == WorkflowStatus.FILLED
            and self.fill is not None
            and self.order is not None
            and self.order.status == PaperOrderStatus.FILLED
        )

    @property
    def is_pending(self) -> bool:
        """True if the order is accepted and pending (e.g. limit order awaiting market price match)."""
        return (
            self.status == WorkflowStatus.ACCEPTED
            and self.fill is None
            and self.order is not None
            and self.order.status == PaperOrderStatus.ACCEPTED
        )

    @property
    def is_rejected(self) -> bool:
        """True if the workflow was rejected."""
        return self.status == WorkflowStatus.REJECTED

    @property
    def is_cancelled(self) -> bool:
        """True if the workflow order was cancelled."""
        return self.status == WorkflowStatus.CANCELLED

    def to_dict(self) -> Dict[str, Any]:
        """Convert outcome to dictionary representation."""
        return {
            "workflow_id": self.workflow_id,
            "correlation_id": self.correlation_id,
            "client_order_id": self.client_order_id,
            "status": self.status.value,
            "success": self.success,
            "stage": self.stage.value,
            "is_filled": self.is_filled,
            "is_pending": self.is_pending,
            "order": self.order.to_dict() if self.order else None,
            "fill": self.fill.to_dict() if self.fill else None,
            "rejection_code": self.rejection_code,
            "rejection_reason": self.rejection_reason,
            "reconciliation_result": self.reconciliation_result,
            "audit_trail": list(self.audit_trail),
            "timestamp": self.timestamp,
            "session_id": self.session_id,
            "sleeve_id": self.sleeve_id,
            "error_message": self.error_message,
        }


@dataclass
class PaperSimulationSession:
    """Lightweight session container grouping explicitly submitted paper orders."""

    session_id: str
    name: str
    created_at: float
    initial_cash: float
    status: str = "ACTIVE"
    stopped_at: Optional[float] = None
    order_ids: List[str] = field(default_factory=list)
    workflow_ids: List[str] = field(default_factory=list)
    notes: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def initial_capital(self) -> float:
        """Alias for initial_cash."""
        return self.initial_cash

    def to_dict(self) -> Dict[str, Any]:
        """Convert session to dictionary."""
        return {
            "session_id": self.session_id,
            "name": self.name,
            "status": self.status,
            "created_at": self.created_at,
            "stopped_at": self.stopped_at,
            "initial_cash": self.initial_cash,
            "order_ids": list(self.order_ids),
            "workflow_ids": list(self.workflow_ids),
            "notes": self.notes,
            "metadata": dict(self.metadata),
        }

    def summary(self, portfolio: PaperPortfolio, ledger: PaperLedger) -> Dict[str, Any]:
        """Generate a complete financial and operational summary using existing portfolio and ledger."""
        recon = ledger.reconcile(expected_current_cash=portfolio.available_cash)
        return {
            "session_id": self.session_id,
            "name": self.name,
            "status": self.status,
            "label": "PAPER ONLY",
            "created_at": self.created_at,
            "stopped_at": self.stopped_at,
            "initial_cash": self.initial_cash,
            "current_cash": portfolio.available_cash,
            "portfolio_equity": portfolio.total_equity,
            "realized_pnl": portfolio.realized_pnl,
            "unrealized_pnl": portfolio.unrealized_pnl,
            "positions": [p.to_dict() for p in portfolio.positions.values()],
            "total_orders": len(self.order_ids),
            "reconciliation": {
                "is_balanced": recon["is_balanced"],
                "discrepancy": recon["discrepancy"],
                "computed_balance": recon["computed_balance"],
                "expected_balance": recon["expected_balance"],
            },
            "financial_digest": compute_financial_digest(portfolio, ledger),
        }


class PaperTradingWorkflowCoordinator:
    """Small workflow coordinator orchestrating explicit paper trading requests with auditability."""

    def __init__(
        self,
        engine: Optional[PaperExecutionEngine] = None,
        settings: Optional[Settings] = None,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        market_service: Optional[Any] = None,
        name: str = "paper_workflow_coordinator",
        enabled: Optional[bool] = None,
        metrics_tracker: Optional[OrderLifecycleTracker] = None,
        calibrator: Optional[HistoricalSpreadCalibrator] = None,
        allocation_manager: Optional[Any] = None,
        outbox: Optional[TransactionalOutbox] = None,
        db_path: Optional[str] = None,
    ) -> None:
        self.name = name
        self.settings = settings or Settings()
        self.engine = engine
        self.event_bus = event_bus or (engine.event_bus if engine else None)
        self.state_store = state_store or (engine.state_store if engine else None)
        self.market_service = market_service

        # Attach metrics tracker and calibrator
        self.metrics_tracker = metrics_tracker or (
            self.engine.metrics_tracker if self.engine and hasattr(self.engine, "metrics_tracker") else OrderLifecycleTracker()
        )
        if self.engine and hasattr(self.engine, "metrics_tracker"):
            self.engine.metrics_tracker = self.metrics_tracker

        self.calibrator = calibrator or HistoricalSpreadCalibrator(
            risk_config=self.engine.risk_config if self.engine else None
        )

        # Attach PortfolioAllocationManager
        if allocation_manager is not None:
            self.allocation_manager = allocation_manager
        elif self.engine is not None:
            from app.execution.allocation import PortfolioAllocationManager
            self.allocation_manager = PortfolioAllocationManager(
                portfolio=self.engine.portfolio,
                ledger=self.engine.ledger,
                risk_config=self.engine.risk_config,
                event_bus=self.event_bus,
                broker_adapter=self.engine.broker_adapter,
            )
        else:
            self.allocation_manager = None

        # Attach TransactionalOutbox
        if outbox is not None:
            self.outbox = outbox
        elif self.allocation_manager is not None and getattr(self.allocation_manager, "outbox", None) is not None:
            self.outbox = self.allocation_manager.outbox
        elif self.engine is not None and getattr(self.engine, "ledger", None) is not None and getattr(self.engine.ledger, "outbox", None) is not None:
            self.outbox = self.engine.ledger.outbox
        else:
            self.outbox = None

        self.db_path = db_path or (getattr(self.engine.ledger, "db_path", None) if self.engine and getattr(self.engine, "ledger", None) else None)

        # Enabled flag respects explicit override or system settings
        self._enabled = (
            enabled
            if enabled is not None
            else getattr(self.settings, "paper_execution_enabled", False)
        )

        self._lock = threading.RLock()
        self._outcomes: Dict[str, PaperWorkflowOutcome] = {}
        self._outcomes_by_client_id: Dict[str, PaperWorkflowOutcome] = {}
        self._sessions: Dict[str, PaperSimulationSession] = {}
        self._active_session: Optional[PaperSimulationSession] = None
        self._last_stress_report: Optional[Any] = None

        if self.db_path:
            self._init_session_db()
            self.recover_sessions()

    @property
    def is_enabled(self) -> bool:
        """Check whether paper trading workflow is actively enabled."""
        with self._lock:
            # Preserves foundational safety invariant: execution_locked must remain True
            if not getattr(self.settings, "execution_locked", True):
                logger.error("CRITICAL: Safety invariant execution_locked is False! Paper workflow disabled.")
                return False

            if not self._enabled:
                return False

            if self.engine is None or not self.engine.is_enabled:
                return False

            return True

    def set_enabled(self, enabled: bool) -> None:
        """Explicitly enable or disable paper workflow coordination."""
        with self._lock:
            self._enabled = bool(enabled)
            if self.engine is not None:
                self.engine.set_enabled(bool(enabled))

    def _publish_event(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        correlation_id: str,
    ) -> None:
        """Publish workflow event across the EventBus with strict error surfacing.

        Raises:
            Exception: Surfaces event publication failure clearly so workflow cannot report false success.
        """
        if self.event_bus is None:
            return

        try:
            event = Event.create(
                event_type=event_type,
                source="paper_workflow_coordinator",
                correlation_id=correlation_id,
                payload=payload,
                metadata={
                    "execution_mode": "PAPER",
                    "execution_locked": self.settings.execution_locked,
                    "broker_provider": self.settings.broker_provider,
                },
            )
            self.event_bus.publish(event)
        except Exception as exc:
            logger.error("Event publication failed for event_type %s [cid=%s]: %s", event_type, correlation_id, exc)
            raise

    def _init_session_db(self) -> None:
        """Initialize SQLite table for simulation session persistence."""
        if not self.db_path:
            return
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            with sqlite3.connect(self.db_path, timeout=10.0) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS paper_simulation_sessions (
                        session_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        stopped_at REAL,
                        initial_cash REAL NOT NULL,
                        notes TEXT,
                        metadata_json TEXT
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_paper_session_status ON paper_simulation_sessions(status)"
                )
        except Exception as err:
            logger.error("Failed to initialize paper sessions SQLite table at %s: %s", self.db_path, err)

    def recover_sessions(self) -> int:
        """Recover persisted simulation sessions from SQLite. Idempotent."""
        if not self.db_path or not Path(self.db_path).is_file():
            return 0
        with self._lock:
            recovered_count = 0
            try:
                with sqlite3.connect(self.db_path, timeout=5.0) as conn:
                    cursor = conn.execute(
                        """
                        SELECT session_id, name, status, created_at, stopped_at,
                               initial_cash, notes, metadata_json
                        FROM paper_simulation_sessions
                        ORDER BY created_at ASC
                        """
                    )
                    for row in cursor.fetchall():
                        (sess_id, name, status, c_at, st_at, init_c, notes, meta_json) = row
                        try:
                            meta = json.loads(meta_json) if meta_json else {}
                        except Exception:
                            meta = {}
                        sess = PaperSimulationSession(
                            session_id=sess_id,
                            name=name,
                            created_at=c_at,
                            initial_cash=init_c,
                            status=status,
                            stopped_at=st_at,
                            notes=notes,
                            metadata=meta,
                        )
                        self._sessions[sess_id] = sess
                        if status == "ACTIVE":
                            self._active_session = sess
                        recovered_count += 1
            except Exception as err:
                logger.error("Failed to recover simulation sessions from SQLite: %s", err)
            return recovered_count

    def _get_db_connection(self) -> sqlite3.Connection:
        """Get an active SQLite connection for workflow and session persistence."""
        if not self.db_path:
            raise RuntimeError("Database path not configured.")
        return sqlite3.connect(self.db_path, timeout=10.0)

    def start_session(
        self,
        name: str = "paper_simulation_session",
        notes: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        session_id: Optional[str] = None,
        initial_capital: Optional[float] = None,
    ) -> PaperSimulationSession:
        """Start a new manual paper simulation session with transactional outbox event delivery."""
        with self._lock:
            if session_id is not None:
                clean_id = session_id.strip()
                if clean_id in self._sessions:
                    return self._sessions[clean_id]
                sess_id = clean_id
            else:
                sess_id = f"psess_{uuid.uuid4().hex[:12]}"

            if self._active_session is not None and self._active_session.status == "ACTIVE":
                self.stop_session(self._active_session.session_id)

            if initial_capital is not None:
                init_cash = float(initial_capital)
            else:
                init_cash = self.engine.portfolio.available_cash if self.engine else getattr(self.settings, "paper_initial_cash", 100_000.0)
            now = time.time()

            session = PaperSimulationSession(
                session_id=sess_id,
                name=name,
                created_at=now,
                initial_cash=init_cash,
                status="ACTIVE",
                notes=notes,
                metadata=dict(metadata or {}),
            )

            outbox_rec = OutboxRecord(
                event_id=f"outbox_sess_start_{sess_id}",
                event_type=EventType.PAPER_SESSION_STARTED.value,
                schema_version=1,
                correlation_id=sess_id,
                causation_id=sess_id,
                payload=session.to_dict(),
                created_at=now,
            )

            if self.db_path:
                try:
                    with self._get_db_connection() as conn:
                        with conn:
                            conn.execute(
                                """
                                INSERT OR REPLACE INTO paper_simulation_sessions (
                                    session_id, name, status, created_at, stopped_at,
                                    initial_cash, notes, metadata_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    session.session_id,
                                    session.name,
                                    session.status,
                                    session.created_at,
                                    session.stopped_at,
                                    session.initial_cash,
                                    session.notes,
                                    json.dumps(session.metadata),
                                ),
                            )
                            if self.outbox:
                                self.outbox.stage_record(outbox_rec, conn=conn)
                except Exception as err:
                    logger.error("Failed to commit session start to SQLite: %s", err)
                    raise
            elif self.outbox:
                self.outbox.stage_record(outbox_rec)

            self._sessions[sess_id] = session
            self._active_session = session

            if self.event_bus:
                try:
                    self.event_bus.publish(outbox_rec.to_event())
                    if self.outbox:
                        self.outbox.mark_delivered(outbox_rec.event_id, delivered_at=time.time())
                except Exception as exc:
                    logger.warning(
                        "Post-commit session start event publication failed for %s: %s (session remains committed)",
                        sess_id,
                        exc,
                    )
                    if self.outbox:
                        self.outbox.record_failure(outbox_rec.event_id, error=str(exc), next_retry_at=time.time() + 2.0)

            logger.info("Started paper simulation session '%s' (id=%s, init_cash=%.2f)", name, sess_id, init_cash)
            return session

    def stop_session(self, session_id: str, reason: Optional[str] = None) -> Optional[PaperSimulationSession]:
        """Stop an active paper simulation session with transactional outbox event delivery."""
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return None

            if session.status == "STOPPED":
                # Idempotent: already stopped
                return session

            now = time.time()
            session.status = "STOPPED"
            session.stopped_at = now
            if reason:
                session.notes = reason

            if self._active_session and self._active_session.session_id == session_id:
                self._active_session = None

            stop_payload = session.to_dict()
            if reason:
                stop_payload["reason"] = reason

            outbox_rec = OutboxRecord(
                event_id=f"outbox_sess_stop_{session_id}",
                event_type=EventType.PAPER_SESSION_STOPPED.value,
                schema_version=1,
                correlation_id=session_id,
                causation_id=session_id,
                payload=stop_payload,
                created_at=now,
            )

            if self.db_path:
                try:
                    with self._get_db_connection() as conn:
                        with conn:
                            conn.execute(
                                """
                                UPDATE paper_simulation_sessions
                                SET status = ?, stopped_at = ?, notes = ?
                                WHERE session_id = ?
                                """,
                                ("STOPPED", now, session.notes, session_id),
                            )
                            if self.outbox:
                                self.outbox.stage_record(outbox_rec, conn=conn)
                except Exception as err:
                    logger.error("Failed to commit session stop to SQLite: %s", err)
                    raise
            elif self.outbox:
                self.outbox.stage_record(outbox_rec)

            if self.event_bus:
                try:
                    self.event_bus.publish(outbox_rec.to_event())
                    if self.outbox:
                        self.outbox.mark_delivered(outbox_rec.event_id, delivered_at=time.time())
                except Exception as exc:
                    logger.warning(
                        "Post-commit session stop event publication failed for %s: %s (session remains committed)",
                        session_id,
                        exc,
                    )
                    if self.outbox:
                        self.outbox.record_failure(outbox_rec.event_id, error=str(exc), next_retry_at=time.time() + 2.0)

            logger.info("Stopped paper simulation session (id=%s)", session_id)
            return session

    def list_sessions(self) -> List[PaperSimulationSession]:
        """Return all tracked paper simulation sessions."""
        with self._lock:
            return list(self._sessions.values())

    def create_equity_snapshot(
        self,
        snapshot_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        source: str = "manual",
        reference_time: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> PortfolioEquitySnapshot:
        """Create and durably persist a portfolio equity snapshot via the allocation manager."""
        if self.allocation_manager is not None:
            return self.allocation_manager.create_equity_snapshot(
                snapshot_id=snapshot_id,
                correlation_id=correlation_id,
                source=source,
                reference_time=reference_time,
                reason=reason,
            )
        raise ValidationError("PortfolioAllocationManager is required for equity snapshot creation.")

    def get_equity_snapshot(self, snapshot_id: str) -> Optional[PortfolioEquitySnapshot]:
        """Retrieve point-in-time equity snapshot."""
        if self.allocation_manager is not None:
            return self.allocation_manager.get_equity_snapshot(snapshot_id)
        return None

    def list_equity_snapshots(self, limit: int = 50, offset: int = 0) -> List[PortfolioEquitySnapshot]:
        """List historical equity snapshots with pagination."""
        if self.allocation_manager is not None:
            return self.allocation_manager.list_equity_snapshots(limit=limit, offset=offset)
        return []

    def get_session(self, session_id: str) -> Optional[PaperSimulationSession]:
        """Retrieve simulation session by ID."""
        with self._lock:
            return self._sessions.get(session_id)

    def get_active_session(self) -> Optional[PaperSimulationSession]:
        """Retrieve the currently active simulation session."""
        with self._lock:
            return self._active_session

    def get_session_summary(self, session_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Retrieve financial and operational summary for session."""
        with self._lock:
            target_id = session_id or (self._active_session.session_id if self._active_session else None)
            if not target_id or target_id not in self._sessions:
                return None
            session = self._sessions[target_id]
            if not self.engine:
                return session.to_dict()
            return session.summary(self.engine.portfolio, self.engine.ledger)

    def submit_paper_order(
        self,
        request: PaperWorkflowRequest,
        market_quote: Optional[Quote] = None,
        reference_time: Optional[float] = None,
    ) -> PaperWorkflowOutcome:
        """Coordinate an explicit paper-order request through all 8 lifecycle stages.

        Stages:
        1. Operational Enablement Validation
        2. Request Structure & Parameter Validation
        3. Market Data Symbol Resolution
        4. Market Data Quality & Freshness Assessment
        5. Execution Engine & Pre-Trade Risk Submission
        6. Structured Result Reception (ACCEPTED, FILLED, REJECTED)
        7. State & Ledger Reconciliation Verification
        8. Structured Workflow Outcome Return & Audit Persistence
        """
        if reference_time is not None:
            if not math.isfinite(reference_time) or reference_time <= 0:
                raise ValidationError(f"reference_time must be a positive finite number, got {reference_time}")
        now = reference_time if reference_time is not None else time.time()
        workflow_id = f"pwf_{uuid.uuid4().hex[:12]}"
        cid = request.correlation_id or uuid.uuid4().hex
        client_order_id = request.client_order_id or f"pord_{uuid.uuid4().hex[:12]}"
        audit_trail: List[str] = [f"REQUEST_RECEIVED at {now:.4f}"]

        with self._lock:
            # Stage 0: Lifecycle Event: Workflow Requested
            t_workflow_start = self.metrics_tracker.profiler.now()
            self.metrics_tracker.record_stage(
                stage=OrderLifecycleStage.REQUEST_RECEIVED,
                symbol=request.symbol,
                status="RECEIVED",
                client_order_id=client_order_id,
                correlation_id=cid,
                session_id=request.session_id,
                timestamp=now,
            )
            try:
                self._publish_event(
                    EventType.PAPER_WORKFLOW_REQUESTED,
                    {
                        "workflow_id": workflow_id,
                        "client_order_id": client_order_id,
                        "symbol": request.symbol,
                        "side": request.side.value,
                        "order_type": request.order_type.value,
                        "quantity": request.quantity,
                        "limit_price": request.limit_price,
                        "session_id": request.session_id,
                    },
                    correlation_id=cid,
                )
            except Exception as pub_err:
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    details={"error": str(pub_err)},
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                return PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.FAILED,
                    success=False,
                    stage=WorkflowStage.REQUEST_RECEIVED,
                    error_message=f"Audit event publication failed: {pub_err}",
                    audit_trail=tuple(audit_trail),
                )

            # Stage 1: Validate Paper Execution is Explicitly Enabled
            t_val_start = self.metrics_tracker.profiler.now()
            if not self.is_enabled:
                t_val_end = self.metrics_tracker.profiler.now()
                self.metrics_tracker.profiler.record_duration(LatencyStage.REQUEST_VALIDATION, t_val_end - t_val_start)
                rejection_reason = "Paper execution is disabled in system settings (PAPER_EXECUTION_ENABLED=false)"
                audit_trail.append("REJECTED: Paper execution disabled")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=request.symbol,
                    status="REJECTED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED.value,
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED.value,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.REQUEST_RECEIVED,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED.value,
                    rejection_reason=rejection_reason,
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_rejection_event(outcome)
                return outcome

            # Stage 2: Idempotency Check & Request Validation
            if client_order_id in self._outcomes_by_client_id:
                t_val_end = self.metrics_tracker.profiler.now()
                self.metrics_tracker.profiler.record_duration(LatencyStage.REQUEST_VALIDATION, t_val_end - t_val_start)
                existing_outcome = self._outcomes_by_client_id[client_order_id]
                audit_trail.append(f"REJECTED: Duplicate client_order_id '{client_order_id}'")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=request.symbol,
                    status="REJECTED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.DUPLICATE_ORDER.value,
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.DUPLICATE_ORDER.value,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.VALIDATION,
                    order=existing_outcome.order,
                    fill=existing_outcome.fill,
                    rejection_code=PaperRejectionCode.DUPLICATE_ORDER.value,
                    rejection_reason=f"Order with client_order_id '{client_order_id}' has already been processed",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_rejection_event(outcome)
                return outcome

            # Validate engine presence
            assert self.engine is not None, "Engine must be attached when coordinator is enabled"

            audit_trail.append("VALIDATION: Request syntax and parameters verified")
            t_val_end = self.metrics_tracker.profiler.now()
            self.metrics_tracker.profiler.record_duration(LatencyStage.REQUEST_VALIDATION, t_val_end - t_val_start)
            self.metrics_tracker.record_stage(
                stage=OrderLifecycleStage.REQUEST_VALIDATED,
                symbol=request.symbol,
                status="VALIDATED",
                client_order_id=client_order_id,
                correlation_id=cid,
                session_id=request.session_id,
                timestamp=now,
            )

            try:
                self._publish_event(
                    EventType.PAPER_WORKFLOW_VALIDATED,
                    {
                        "workflow_id": workflow_id,
                        "client_order_id": client_order_id,
                        "symbol": request.symbol,
                        "validation_status": "PASSED",
                    },
                    correlation_id=cid,
                )
            except Exception as pub_err:
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    details={"error": str(pub_err)},
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                return PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.FAILED,
                    success=False,
                    stage=WorkflowStage.VALIDATION,
                    error_message=f"Validation event publication failed: {pub_err}",
                    audit_trail=tuple(audit_trail),
                )

            # Stage 3: Resolve Requested Symbol Against Eligible Market Data
            t_quote_start = self.metrics_tracker.profiler.now()
            quote = market_quote or getattr(request, "quote", None)
            if quote is None:
                quote = self.engine.get_quote(request.symbol)

            if quote is None and self.market_service is not None:
                cached_quotes = self.market_service.state_store.get("latest_quotes", {})
                raw_quote = cached_quotes.get(request.symbol)
                if raw_quote:
                    try:
                        quote = Quote(
                            symbol=raw_quote["symbol"],
                            exchange=raw_quote.get("exchange", "MOCK"),
                            timestamp=float(raw_quote["timestamp"]),
                            last_price=float(raw_quote["last_price"]),
                            bid=raw_quote.get("bid"),
                            ask=raw_quote.get("ask"),
                            bid_size=raw_quote.get("bid_size"),
                            ask_size=raw_quote.get("ask_size"),
                            volume=float(raw_quote.get("volume", 0.0)),
                            source=raw_quote.get("source", "mock"),
                        )
                    except Exception as err:
                        logger.error("Failed to parse quote from market_service: %s", err)

            if quote is None:
                t_quote_end = self.metrics_tracker.profiler.now()
                self.metrics_tracker.profiler.record_duration(LatencyStage.QUOTE_VALIDATION, t_quote_end - t_quote_start)
                audit_trail.append(f"REJECTED: Missing market data for '{request.symbol}'")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=request.symbol,
                    status="REJECTED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.MISSING_MARKET_DATA.value,
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.MISSING_MARKET_DATA.value,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.MARKET_DATA_RESOLUTION,
                    rejection_code=PaperRejectionCode.MISSING_MARKET_DATA.value,
                    rejection_reason=f"No eligible market quote available for symbol '{request.symbol}'",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_rejection_event(outcome)
                return outcome

            # Stage 4: Check Market Data Quality, Timestamp Freshness, and Symbol Consistency
            if quote.symbol.upper() != request.symbol.upper():
                t_quote_end = self.metrics_tracker.profiler.now()
                self.metrics_tracker.profiler.record_duration(LatencyStage.QUOTE_VALIDATION, t_quote_end - t_quote_start)
                audit_trail.append(f"REJECTED: Quote symbol mismatch '{quote.symbol}' != '{request.symbol}'")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=request.symbol,
                    status="REJECTED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.MARKET_DATA_RESOLUTION,
                    rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                    rejection_reason=f"Market quote symbol '{quote.symbol}' does not match requested symbol '{request.symbol}'",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_rejection_event(outcome)
                return outcome

            if not math.isfinite(quote.last_price) or quote.last_price <= 0:
                t_quote_end = self.metrics_tracker.profiler.now()
                self.metrics_tracker.profiler.record_duration(LatencyStage.QUOTE_VALIDATION, t_quote_end - t_quote_start)
                audit_trail.append(f"REJECTED: Quote price non-positive/finite: {quote.last_price}")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=request.symbol,
                    status="REJECTED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.MARKET_DATA_RESOLUTION,
                    rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                    rejection_reason=f"Market quote has invalid price: {quote.last_price}",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_rejection_event(outcome)
                return outcome

            if quote.bid is not None and quote.ask is not None and quote.bid > 0 and quote.ask > 0:
                if quote.ask < quote.bid:
                    t_quote_end = self.metrics_tracker.profiler.now()
                    self.metrics_tracker.profiler.record_duration(LatencyStage.QUOTE_VALIDATION, t_quote_end - t_quote_start)
                    audit_trail.append("REJECTED: Inverted bid-ask spread")
                    self.metrics_tracker.record_stage(
                        stage=OrderLifecycleStage.ORDER_REJECTED,
                        symbol=request.symbol,
                        status="REJECTED",
                        client_order_id=client_order_id,
                        correlation_id=cid,
                        session_id=request.session_id,
                        timestamp=now,
                        rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                    )
                    self.metrics_tracker.record_stage(
                        stage=OrderLifecycleStage.WORKFLOW_FAILED,
                        symbol=request.symbol,
                        status="FAILED",
                        client_order_id=client_order_id,
                        correlation_id=cid,
                        session_id=request.session_id,
                        timestamp=now,
                        rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                    )
                    self.metrics_tracker.profiler.record_duration(
                        LatencyStage.WORKFLOW_E2E,
                        self.metrics_tracker.profiler.now() - t_workflow_start,
                    )
                    outcome = PaperWorkflowOutcome(
                        workflow_id=workflow_id,
                        correlation_id=cid,
                        client_order_id=client_order_id,
                        status=WorkflowStatus.REJECTED,
                        success=False,
                        stage=WorkflowStage.MARKET_DATA_RESOLUTION,
                        rejection_code=PaperRejectionCode.INVALID_MARKET_DATA.value,
                        rejection_reason=f"Market quote has inverted spread (ask {quote.ask} < bid {quote.bid})",
                        audit_trail=tuple(audit_trail),
                        session_id=request.session_id,
                    )
                    self._record_rejection_event(outcome)
                    return outcome

            freshness_threshold = getattr(self.engine.risk_config, "freshness_threshold_seconds", 60.0)
            data_age = max(0.0, now - quote.timestamp)
            if data_age > freshness_threshold:
                t_quote_end = self.metrics_tracker.profiler.now()
                self.metrics_tracker.profiler.record_duration(LatencyStage.QUOTE_VALIDATION, t_quote_end - t_quote_start)
                audit_trail.append(f"REJECTED: Stale market quote (age={data_age:.1f}s > {freshness_threshold:.1f}s)")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=request.symbol,
                    status="REJECTED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.STALE_MARKET_DATA.value,
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=PaperRejectionCode.STALE_MARKET_DATA.value,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.MARKET_DATA_RESOLUTION,
                    rejection_code=PaperRejectionCode.STALE_MARKET_DATA.value,
                    rejection_reason=f"Market quote for '{request.symbol}' is stale (age={data_age:.1f}s, threshold={freshness_threshold:.1f}s)",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_rejection_event(outcome)
                return outcome

            t_quote_end = self.metrics_tracker.profiler.now()
            self.metrics_tracker.profiler.record_duration(LatencyStage.QUOTE_VALIDATION, t_quote_end - t_quote_start)
            self.metrics_tracker.record_stage(
                stage=OrderLifecycleStage.QUOTE_VALIDATED,
                symbol=request.symbol,
                status="VALIDATED",
                client_order_id=client_order_id,
                correlation_id=cid,
                session_id=request.session_id,
                timestamp=now,
            )
            # Add eligible observation to spread calibrator
            self.calibrator.add_observation(quote, reference_time=now)
            audit_trail.append(f"MARKET_DATA_RESOLUTION: Verified quote for '{quote.symbol}' at {quote.last_price}")

            # Stage 5: Submit Request to Existing Paper Execution Engine & Risk Gate
            engine_order_req = PaperOrderRequest(
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                limit_price=request.limit_price,
                client_order_id=client_order_id,
                correlation_id=cid,
                notes=request.notes,
            )

            try:
                if request.sleeve_id and self.allocation_manager is not None:
                    exec_result = self.allocation_manager.execute_sleeve_order(
                        sleeve_id=request.sleeve_id,
                        request=engine_order_req,
                        quote=quote,
                        reference_time=now,
                    )
                else:
                    exec_result = self.engine.submit_order(
                        request=engine_order_req,
                        market_quote=quote,
                        reference_time=now,
                    )
            except Exception as engine_exc:
                logger.error("Engine order submission threw exception: %s", engine_exc)
                audit_trail.append(f"FAILED: Engine exception: {engine_exc}")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    details={"error": str(engine_exc)},
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.FAILED,
                    success=False,
                    stage=WorkflowStage.SIMULATED_EXECUTION,
                    error_message=str(engine_exc),
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )
                self._record_failure_event(outcome)
                return outcome

            # Stage 6: Structured Result Evaluation
            order = exec_result.order
            fill = exec_result.fill

            if order.status == PaperOrderStatus.REJECTED:
                rej_code = (
                    exec_result.rejection_code.value
                    if exec_result.rejection_code
                    else (order.rejection_code.value if order.rejection_code else "RISK_REJECTED")
                )
                rej_reason = exec_result.rejection_reason or order.rejection_reason or "Order rejected by risk gate"
                audit_trail.append(f"REJECTED: {rej_code} - {rej_reason}")

                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="REJECTED",
                    order_id=order.order_id,
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    rejection_code=rej_code,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.RISK_GATE,
                    order=order,
                    fill=None,
                    rejection_code=rej_code,
                    rejection_reason=rej_reason,
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                    sleeve_id=request.sleeve_id,
                )
                self._record_rejection_event(outcome)
                self._outcomes[workflow_id] = outcome
                self._outcomes_by_client_id[client_order_id] = outcome
                return outcome

            elif order.status == PaperOrderStatus.ACCEPTED:
                # Order is accepted and pending (e.g. limit order awaiting price match)
                audit_trail.append(f"ACCEPTED: Pending limit order {order.order_id} at limit_price {order.limit_price}")
                workflow_status = WorkflowStatus.ACCEPTED

            elif order.status == PaperOrderStatus.FILLED:
                # Order is filled
                assert fill is not None, "FILLED order must have an associated fill record"
                audit_trail.append(f"FILLED: Order {order.order_id} filled {fill.quantity} @ {fill.price} (fee={fill.fee})")
                workflow_status = WorkflowStatus.FILLED

            else:
                audit_trail.append(f"UNEXPECTED_STATUS: {order.status}")
                workflow_status = WorkflowStatus.FAILED

            # Stage 7: State Reconciliation & Verification
            recon = self.engine.ledger.reconcile(expected_current_cash=self.engine.portfolio.available_cash)
            if not recon["is_balanced"]:
                audit_trail.append(f"RECONCILIATION_FAILED: Discrepancy of {recon['discrepancy']} between portfolio and ledger")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.RECONCILIATION_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    order_id=order.order_id if order else None,
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                    details={"discrepancy": recon["discrepancy"]},
                )
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_FAILED,
                    symbol=request.symbol,
                    status="FAILED",
                    order_id=order.order_id if order else None,
                    client_order_id=client_order_id,
                    correlation_id=cid,
                    session_id=request.session_id,
                    timestamp=now,
                )
                self.metrics_tracker.profiler.record_duration(
                    LatencyStage.WORKFLOW_E2E,
                    self.metrics_tracker.profiler.now() - t_workflow_start,
                )
                reconciliation_failed_outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.RECONCILIATION_FAILED,
                    success=False,
                    stage=WorkflowStage.STATE_RECONCILIATION,
                    order=order,
                    fill=fill,
                    reconciliation_result=recon,
                    error_message=f"Ledger reconciliation failed: discrepancy={recon['discrepancy']}",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                    sleeve_id=request.sleeve_id,
                )
                try:
                    self._publish_event(
                        EventType.PAPER_RECONCILIATION_FAILED,
                        reconciliation_failed_outcome.to_dict(),
                        correlation_id=cid,
                    )
                except Exception as pub_err:
                    logger.error("Failed to publish PAPER_RECONCILIATION_FAILED event: %s", pub_err)

                self._outcomes[workflow_id] = reconciliation_failed_outcome
                self._outcomes_by_client_id[client_order_id] = reconciliation_failed_outcome
                return reconciliation_failed_outcome

            # Verify integrity invariants for fill
            if workflow_status == WorkflowStatus.FILLED:
                assert fill is not None
                if abs(order.filled_quantity - fill.quantity) > 1e-6:
                    raise ValidationError(f"Order filled quantity {order.filled_quantity} mismatch with fill {fill.quantity}")
                if order.fill_price is None or abs(order.fill_price - fill.price) > 1e-6:
                    raise ValidationError(f"Order fill price {order.fill_price} mismatch with fill {fill.price}")

            audit_trail.append("STATE_RECONCILIATION: Portfolio and ledger balance verified")
            self.metrics_tracker.record_stage(
                stage=OrderLifecycleStage.PORTFOLIO_RECONCILED,
                symbol=request.symbol,
                status="RECONCILED",
                order_id=order.order_id if order else None,
                client_order_id=client_order_id,
                correlation_id=cid,
                session_id=request.session_id,
                timestamp=now,
            )

            # Stage 8: Structured Workflow Outcome Return & Audit Persistence
            outcome = PaperWorkflowOutcome(
                workflow_id=workflow_id,
                correlation_id=cid,
                client_order_id=client_order_id,
                status=workflow_status,
                success=True,
                stage=WorkflowStage.COMPLETED,
                order=order,
                fill=fill,
                reconciliation_result=recon,
                audit_trail=tuple(audit_trail),
                session_id=request.session_id,
                sleeve_id=request.sleeve_id,
            )

            # Record in session if specified or if an active session exists
            sess_id = request.session_id or (self._active_session.session_id if self._active_session else None)
            if sess_id and sess_id in self._sessions:
                sess = self._sessions[sess_id]
                sess.workflow_ids.append(workflow_id)
                if order:
                    sess.order_ids.append(order.order_id)

            self._outcomes[workflow_id] = outcome
            self._outcomes_by_client_id[client_order_id] = outcome
            self.metrics_tracker.record_stage(
                stage=OrderLifecycleStage.WORKFLOW_COMPLETED,
                symbol=request.symbol,
                status=workflow_status.value,
                order_id=order.order_id if order else None,
                client_order_id=client_order_id,
                correlation_id=cid,
                session_id=request.session_id,
                timestamp=now,
            )
            self.metrics_tracker.profiler.record_duration(
                LatencyStage.WORKFLOW_E2E,
                self.metrics_tracker.profiler.now() - t_workflow_start,
            )

            # Publish Workflow Completed Event
            try:
                self._publish_event(
                    EventType.PAPER_WORKFLOW_COMPLETED,
                    outcome.to_dict(),
                    correlation_id=cid,
                )
            except Exception as pub_err:
                return PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=client_order_id,
                    status=WorkflowStatus.FAILED,
                    success=False,
                    stage=WorkflowStage.COMPLETED,
                    order=order,
                    fill=fill,
                    error_message=f"Completion event publication failed: {pub_err}",
                    audit_trail=tuple(audit_trail),
                    session_id=request.session_id,
                )

            return outcome

    submit_order = submit_paper_order

    def execute_workflow(
        self,
        request: PaperWorkflowRequest,
        quote: Optional[Quote] = None,
        market_quote: Optional[Quote] = None,
        reference_time: Optional[float] = None,
    ) -> PaperWorkflowOutcome:
        """Alias for submit_paper_order with flexible quote parameter passing."""
        eff_quote = quote or market_quote or getattr(request, "quote", None)
        return self.submit_paper_order(
            request=request,
            market_quote=eff_quote,
            reference_time=reference_time,
        )

    def cancel_order(
        self,
        order_id: str,
        reason: str = "User cancellation",
        correlation_id: Optional[str] = None,
    ) -> PaperWorkflowOutcome:
        """Cancel an open/pending paper order and reconcile ledger and portfolio."""
        workflow_id = f"pwf_can_{uuid.uuid4().hex[:12]}"
        cid = correlation_id or uuid.uuid4().hex
        audit_trail: List[str] = [f"CANCELLATION_REQUESTED for order {order_id}"]

        with self._lock:
            if not self.is_enabled:
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol="UNKNOWN",
                    status="REJECTED",
                    order_id=order_id,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED.value,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.CANCELLATION,
                    rejection_code=PaperRejectionCode.PAPER_EXECUTION_DISABLED.value,
                    rejection_reason="Paper execution is disabled in system settings",
                    audit_trail=tuple(audit_trail),
                )
                self._record_rejection_event(outcome)
                return outcome

            assert self.engine is not None
            cancel_result = self.engine.cancel_order(order_id, reason=reason)

            if not cancel_result.success:
                code = cancel_result.rejection_code.value if cancel_result.rejection_code else "CANCEL_REJECTED"
                audit_trail.append(f"REJECTED: Cannot cancel order {order_id}: {cancel_result.rejection_reason}")
                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.ORDER_REJECTED,
                    symbol=cancel_result.order.symbol if cancel_result.order else "UNKNOWN",
                    status="REJECTED",
                    order_id=order_id,
                    client_order_id=cancel_result.order.client_order_id if cancel_result.order else None,
                    rejection_code=code,
                )
                outcome = PaperWorkflowOutcome(
                    workflow_id=workflow_id,
                    correlation_id=cid,
                    client_order_id=cancel_result.order.client_order_id,
                    status=WorkflowStatus.REJECTED,
                    success=False,
                    stage=WorkflowStage.CANCELLATION,
                    order=cancel_result.order,
                    rejection_code=code,
                    rejection_reason=cancel_result.rejection_reason,
                    audit_trail=tuple(audit_trail),
                )
                self._record_rejection_event(outcome)
                return outcome

            # Verify reconciliation post-cancellation
            recon = self.engine.ledger.reconcile(expected_current_cash=self.engine.portfolio.available_cash)
            audit_trail.append(f"CANCELLED: Order {order_id} successfully cancelled. Ledger balanced: {recon['is_balanced']}")

            self.metrics_tracker.record_stage(
                stage=OrderLifecycleStage.ORDER_CANCELLED,
                symbol=cancel_result.order.symbol,
                status="CANCELLED",
                order_id=order_id,
                client_order_id=cancel_result.order.client_order_id,
                correlation_id=cid,
                details={"reason": reason},
            )

            outcome = PaperWorkflowOutcome(
                workflow_id=workflow_id,
                correlation_id=cid,
                client_order_id=cancel_result.order.client_order_id,
                status=WorkflowStatus.CANCELLED,
                success=True,
                stage=WorkflowStage.COMPLETED,
                order=cancel_result.order,
                reconciliation_result=recon,
                audit_trail=tuple(audit_trail),
            )
            self._outcomes[workflow_id] = outcome
            self._publish_event(
                EventType.PAPER_WORKFLOW_COMPLETED,
                outcome.to_dict(),
                correlation_id=cid,
            )
            return outcome

    def on_market_quote(self, quote: Quote) -> List[PaperWorkflowOutcome]:
        """Handle incoming market quote, updating engine cache and processing open pending limit orders."""
        with self._lock:
            if not self.is_enabled or self.engine is None:
                return []

            # Feed quote observation to spread calibrator
            self.calibrator.add_observation(quote, reference_time=quote.timestamp)

            # 1. Forward quote to engine and receive results of filled pending limit orders
            exec_results = self.engine.on_market_quote(quote)

            # 2. Convert each filled limit order result into an auditable workflow outcome
            completed_outcomes: List[PaperWorkflowOutcome] = []
            for res in exec_results:
                order = res.order
                fill = res.fill
                client_oid = order.client_order_id
                prior_outcome = self._outcomes_by_client_id.get(client_oid)
                sess_id = prior_outcome.session_id if prior_outcome else None

                recon = self.engine.ledger.reconcile(expected_current_cash=self.engine.portfolio.available_cash)
                new_outcome = PaperWorkflowOutcome(
                    workflow_id=f"pwf_lim_fill_{uuid.uuid4().hex[:8]}",
                    correlation_id=order.correlation_id or uuid.uuid4().hex,
                    client_order_id=client_oid,
                    status=WorkflowStatus.FILLED,
                    success=True,
                    stage=WorkflowStage.COMPLETED,
                    order=order,
                    fill=fill,
                    reconciliation_result=recon,
                    audit_trail=(
                        f"PENDING limit order filled on market quote update at {quote.last_price}",
                        f"Reconciliation balanced: {recon['is_balanced']}",
                    ),
                    session_id=sess_id,
                )
                self._outcomes[new_outcome.workflow_id] = new_outcome
                self._outcomes_by_client_id[client_oid] = new_outcome
                completed_outcomes.append(new_outcome)

                self.metrics_tracker.record_stage(
                    stage=OrderLifecycleStage.WORKFLOW_COMPLETED,
                    symbol=order.symbol,
                    status=WorkflowStatus.FILLED.value,
                    order_id=order.order_id,
                    client_order_id=client_oid,
                    correlation_id=new_outcome.correlation_id,
                )

                self._publish_event(
                    EventType.PAPER_WORKFLOW_COMPLETED,
                    new_outcome.to_dict(),
                    correlation_id=new_outcome.correlation_id,
                )

            return completed_outcomes

    def _record_rejection_event(self, outcome: PaperWorkflowOutcome) -> None:
        """Publish workflow rejection event."""
        try:
            self._publish_event(
                EventType.PAPER_WORKFLOW_REJECTED,
                outcome.to_dict(),
                correlation_id=outcome.correlation_id,
            )
        except Exception as exc:
            logger.error("Failed to publish PAPER_WORKFLOW_REJECTED event: %s", exc)

    def _record_failure_event(self, outcome: PaperWorkflowOutcome) -> None:
        """Publish workflow failure event."""
        try:
            self._publish_event(
                EventType.PAPER_WORKFLOW_FAILED,
                outcome.to_dict(),
                correlation_id=outcome.correlation_id,
            )
        except Exception as exc:
            logger.error("Failed to publish PAPER_WORKFLOW_FAILED event: %s", exc)

    def get_outcome(self, workflow_id: str) -> Optional[PaperWorkflowOutcome]:
        """Lookup workflow outcome by workflow_id."""
        with self._lock:
            return self._outcomes.get(workflow_id)

    def get_outcome_by_client_id(self, client_order_id: str) -> Optional[PaperWorkflowOutcome]:
        """Lookup workflow outcome by client_order_id."""
        with self._lock:
            return self._outcomes_by_client_id.get(client_order_id)

    def get_lifecycle_summary(self) -> Dict[str, Any]:
        """Return comprehensive order lifecycle metrics."""
        return self.metrics_tracker.get_summary()

    def get_latency_profile(self) -> Dict[str, Any]:
        """Return execution latency profile across all workflow stages."""
        return self.metrics_tracker.profiler.get_profile()

    def get_calibration_summary(
        self,
        symbol: Optional[str] = None,
        as_of_time: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Return advisory historical-spread slippage calibration report(s)."""
        if symbol:
            report = self.calibrator.calibrate(symbol, as_of_time=as_of_time)
            return report.to_dict()
        all_reports = self.calibrator.calibrate_all(as_of_time=as_of_time)
        return {sym: rep.to_dict() for sym, rep in all_reports.items()}

    def add_market_observation(
        self,
        quote_or_record: Quote | Dict[str, Any],
        reference_time: Optional[float] = None,
    ) -> Tuple[bool, Optional[str], Optional[SpreadObservation]]:
        """Feed a market observation directly to the calibration component."""
        ok, reason, obs = self.calibrator.add_observation(quote_or_record, reference_time=reference_time)
        return ok, reason.value if reason else None, obs

    def get_summary(self) -> Dict[str, Any]:
        """Provide a minimal, strictly read-only paper workflow summary."""
        with self._lock:
            total_orders = 0
            order_counts = {
                "PENDING": 0,
                "ACCEPTED": 0,
                "FILLED": 0,
                "REJECTED": 0,
                "CANCELLED": 0,
            }
            cash = 0.0
            equity = 0.0
            realized_pnl = 0.0
            unrealized_pnl = 0.0
            positions_data = []
            recon_data = {"is_balanced": True, "discrepancy": 0.0, "computed_balance": 0.0}

            if self.engine is not None:
                orders = self.engine.list_orders()
                total_orders = len(orders)
                for o in orders:
                    s = o.status.value
                    if s in order_counts:
                        order_counts[s] += 1
                p = self.engine.portfolio
                cash = p.available_cash
                equity = p.total_equity
                realized_pnl = p.realized_pnl
                unrealized_pnl = p.unrealized_pnl
                positions_data = [pos.to_dict() for pos in p.positions.values()]
                recon = self.engine.ledger.reconcile(expected_current_cash=cash)
                recon_data = {
                    "is_balanced": recon["is_balanced"],
                    "discrepancy": recon["discrepancy"],
                    "computed_balance": recon["computed_balance"],
                }

            recent_outcomes = [
                outcome.to_dict()
                for outcome in list(self._outcomes.values())[-10:]
            ]

            active_sess = self._active_session.to_dict() if self._active_session else None

            return {
                "status": "success",
                "label": "PAPER ONLY",
                "paper_execution_enabled": self.is_enabled,
                "execution_locked": self.settings.execution_locked,
                "broker_provider": self.settings.broker_provider,
                "trading_engine": "not initialized",
                "total_orders": total_orders,
                "order_counts": order_counts,
                "paper_cash": cash,
                "paper_equity": equity,
                "realized_pnl": realized_pnl,
                "unrealized_pnl": unrealized_pnl,
                "positions": positions_data,
                "ledger_reconciliation": recon_data,
                "active_session": active_sess,
                "recent_outcomes": recent_outcomes,
                "financial_digest": compute_financial_digest(self.engine.portfolio, self.engine.ledger) if self.engine else None,
                "lifecycle_metrics": self.get_lifecycle_summary(),
                "latency_profile": self.get_latency_profile(),
                "calibration_summary": self.get_calibration_summary(),
                "spread_calibration": self.get_calibration_summary(),
                "metrics_digest": self.metrics_tracker.compute_digest(),
                "latency_digest": self.metrics_tracker.profiler.compute_digest(),
                "stress_test_report": self.get_stress_test_report(),
                "portfolio_exposure": self.get_portfolio_exposure().to_dict() if self.get_portfolio_exposure() else None,
                "allocation_reconciliation": self.reconcile_allocations().to_dict() if self.reconcile_allocations() else None,
            }

    def allocate_capital(self, request: Any) -> Any:
        """Execute portfolio capital partitioning across strategy sleeves."""
        with self._lock:
            if self.allocation_manager is None:
                raise ValidationError("PortfolioAllocationManager is not initialized on this coordinator.")
            return self.allocation_manager.apply_allocation(request)

    def get_allocation_manager(self) -> Optional[Any]:
        """Retrieve the attached PortfolioAllocationManager."""
        with self._lock:
            return self.allocation_manager

    def get_portfolio_exposure(self) -> Optional[Any]:
        """Retrieve consolidated multi-sleeve exposure snapshot."""
        with self._lock:
            if self.allocation_manager is not None:
                return self.allocation_manager.get_portfolio_exposure()
            return None

    def get_sleeve_exposure(self, sleeve_id: str) -> Optional[Any]:
        """Retrieve single sleeve exposure snapshot."""
        with self._lock:
            if self.allocation_manager is not None:
                return self.allocation_manager.get_sleeve_exposure(sleeve_id)
            return None

    def reconcile_allocations(self, tolerance: float = 1e-4) -> Optional[Any]:
        """Perform mathematical reconciliation between strategy sleeves and portfolio."""
        with self._lock:
            if self.allocation_manager is not None:
                return self.allocation_manager.reconcile(tolerance=tolerance)
            return None

    def run_stress_scenarios(
        self,
        scenarios: Optional[Sequence[Any]] = None,
        run_sensitivity: bool = True,
    ) -> Any:
        """Execute deterministic multi-scenario stress tests and parameter sensitivity sweeps."""
        from app.execution.stress_testing import run_all_stress_scenarios
        report = run_all_stress_scenarios(
            scenarios=scenarios,
            settings=self.settings,
            run_sensitivity=run_sensitivity,
        )
        with self._lock:
            self._last_stress_report = report
        return report

    def get_stress_test_report(self) -> Optional[Dict[str, Any]]:
        """Retrieve the latest cached multi-scenario stress test report, if available."""
        with self._lock:
            if self._last_stress_report is not None:
                return self._last_stress_report.to_dict()
            return None

    # Aliases for consistency
    execute_order = submit_paper_order
    export_workflow_diagnostics = get_summary

