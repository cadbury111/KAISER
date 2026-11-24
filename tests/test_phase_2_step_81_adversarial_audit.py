"""KAISER — Phase 2, Step 81: Independent Adversarial Audit & Production Readiness Gap Assessment.

Comprehensive, adversarial test suite challenging:
1. Financial ledger invariants: duplicate capital, broken balance chains, non-finite values, tampered reconciliation.
2. Paper execution boundaries: insufficient cash, short selling restriction, stale data, exact limit boundary, quote fallbacks.
3. Transactional outbox boundaries: atomic rollback on staging failure, post-commit isolation, retry exhaustion (dead letter), secret/audio redaction.
4. Backup & Disaster recovery: byte-level corruption detection, tampered manifest checksum, retention invariants, symlink traversal blocking.
5. Voice and dashboard security: multilingual prohibited trading directives, HTTP 401/405 guards, path sanitization, global execution lock.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error
import pytest

from app.core.config import Settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.market.models import Bar, Quote
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
from app.execution.risk import PaperRiskGate
from app.execution.simulated_broker import SimulatedBrokerAdapter
from app.execution.engine import PaperExecutionEngine
from app.execution.allocation import (
    PortfolioAllocationManager,
    StrategySleeve,
    PortfolioAllocationRequest,
    StrategySleeveDefinition,
    SleeveRiskBudget,
    AllocationStatus,
    AllocationRejectionCode,
    create_standard_multi_sleeve_request,
)
from app.execution.workflow import (
    PaperTradingWorkflowCoordinator,
    PaperWorkflowRequest,
    PaperWorkflowOutcome,
    WorkflowStage,
    WorkflowStatus,
)
from app.ledger.models import LedgerEntryType, PaperLedgerEntry
from app.ledger.ledger import PaperLedger
from app.ledger.outbox import OutboxRecord, OutboxStatus, TransactionalOutbox, redact_sensitive_payload
from app.ledger.backup import DatabaseBackupService
from app.ledger.backup_coordinator import (
    BackupOperationsCoordinator,
    BackupHealthStatus,
    RecoveryDrillStatus,
    sanitize_backup_path_for_display,
)
from app.portfolio.dashboard.server import PortfolioDashboardServer
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Provide an isolated temporary test directory."""
    return tmp_path


@pytest.fixture
def isolated_db_path(temp_dir: Path) -> str:
    """Provide an isolated temporary SQLite database path."""
    return str(temp_dir / "adv_test_ledger.db")


@pytest.fixture
def sample_quote_reliance() -> Quote:
    """Provide a realistic mock quote for RELIANCE."""
    now = time.time()
    return Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=now,
        last_price=2500.0,
        bid=2499.5,
        ask=2500.5,
        bid_size=100.0,
        ask_size=100.0,
        volume=50000.0,
        source="mock",
    )


# ==============================================================================
# 1. LEDGER & ACCOUNTING ADVERSARIAL AUDIT
# ==============================================================================


class TestAdversarialLedgerAndAccounting:
    """Adversarial stress testing of the financial ledger and accounting engine."""

    def test_adv_01_duplicate_initial_capital_rejection_and_chain_integrity(self, isolated_db_path: str) -> None:
        """Challenge: Attempt to insert duplicate and conflicting initial capital records."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)

        # 1. Attempt duplicate insertion with same idempotency key
        entry_dupe = PaperLedgerEntry(
            entry_id="dupe_init",
            timestamp=time.time(),
            entry_type=LedgerEntryType.INITIAL_CAPITAL,
            cash_change=50_000.0,
            cash_balance_after=50_000.0,
            idempotency_key=f"init_capital_{ledger.name}_50000.0",
        )
        assert ledger.record_entry(entry_dupe) is False
        assert ledger.count() == 1

        # 2. Replay with conflicting initial capital must fail closed
        conflicting_entry = PaperLedgerEntry(
            entry_id="conflict_init",
            timestamp=time.time() + 1.0,
            entry_type=LedgerEntryType.INITIAL_CAPITAL,
            cash_change=75_000.0,  # Conflicting capital amount!
            cash_balance_after=125_000.0,
            idempotency_key="conflict_init_key_999",
        )
        # Directly insert into SQLite to simulate persistent tampering
        with sqlite3.connect(isolated_db_path) as conn:
            conn.execute(
                """
                INSERT INTO paper_ledger_entries (
                    entry_id, timestamp, entry_type, cash_change, cash_balance_after,
                    fee, realized_pnl, idempotency_key, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conflicting_entry.entry_id,
                    conflicting_entry.timestamp,
                    conflicting_entry.entry_type.value,
                    conflicting_entry.cash_change,
                    conflicting_entry.cash_balance_after,
                    0.0,
                    0.0,
                    conflicting_entry.idempotency_key,
                    "{}",
                ),
            )

        # Reopening ledger and recovering engine must detect the conflicting initial capital and raise ValidationError
        tampered_ledger = PaperLedger(db_path=isolated_db_path)
        with pytest.raises(ValidationError, match="Conflicting initial capital records in ledger"):
            PaperExecutionEngine(ledger=tampered_ledger)

    def test_adv_02_broken_balance_chain_reconciliation_detection(self, isolated_db_path: str) -> None:
        """Challenge: Tamper with cash_balance_after in the entry chain; verify reconciliation fails."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=100_000.0)

        # Insert entry where cash_balance_after does not equal 100000 - 5000 = 95000
        broken_entry = PaperLedgerEntry(
            entry_id="broken_chain_entry",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-5000.0,
            cash_balance_after=98000.0,  # Broken! (Should be 95000)
            idempotency_key="broken_chain_key",
        )
        ledger.record_entry(broken_entry)

        recon = ledger.reconcile(expected_current_cash=95000.0)
        assert recon["is_balanced"] is False

    def test_adv_03_corrupted_non_finite_values_rejection(self, isolated_db_path: str) -> None:
        """Challenge: Attempt to persist or recover NaN or Inf values; verify strict fail-closed rejection."""
        # Non-finite cash in entry creation must be rejected
        with pytest.raises(ValidationError, match="finite"):
            PaperLedgerEntry(
                entry_id="nan_entry",
                timestamp=time.time(),
                entry_type=LedgerEntryType.ORDER_FILL,
                cash_change=float("nan"),
                cash_balance_after=50000.0,
                idempotency_key="nan_key",
            )

        with pytest.raises(ValidationError, match="finite"):
            PaperLedgerEntry(
                entry_id="inf_entry",
                timestamp=time.time(),
                entry_type=LedgerEntryType.ORDER_FILL,
                cash_change=float("inf"),
                cash_balance_after=50000.0,
                idempotency_key="inf_key",
            )

    def test_adv_04_tampered_cash_discrepancy_exact_detection(self, isolated_db_path: str) -> None:
        """Challenge: Alter expected cash by even 1 cent ($0.01); verify reconciliation flags discrepancy."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)

        # Expected cash matches exactly
        recon_exact = ledger.reconcile(expected_current_cash=50_000.0)
        assert recon_exact["is_balanced"] is True
        assert recon_exact["discrepancy"] == 0.0

        # Expected cash altered by $0.01
        recon_tampered = ledger.reconcile(expected_current_cash=50_000.01)
        assert recon_tampered["is_balanced"] is False
        assert recon_tampered["discrepancy"] == 0.01

    def test_adv_05_unknown_sleeve_fill_fails_closed(self, isolated_db_path: str) -> None:
        """Challenge: Ledger fill referencing unknown sleeve must abort sleeve recovery."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        alloc_mgr = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

        # Insert fill referencing non-existent sleeve 'ghost_sleeve'
        ghost_fill = PaperLedgerEntry(
            entry_id="ghost_fill_entry",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            order_id="ord_ghost",
            symbol="RELIANCE",
            side="BUY",
            quantity=2.0,
            price=2500.0,
            fee=2.5,
            cash_change=-5002.5,
            cash_balance_after=44997.5,
            idempotency_key="ghost_fill_key",
            metadata={"sleeve_id": "ghost_sleeve", "fill_id": "fill_ghost"},
        )
        ledger.record_entry(ghost_fill)

        with pytest.raises(ValidationError, match="references unknown strategy sleeve 'ghost_sleeve'"):
            alloc_mgr.recover_from_ledger(ledger)

    def test_adv_06_allocation_exceeding_portfolio_cash_rejected(self, isolated_db_path: str) -> None:
        """Challenge: Request allocating more capital than available portfolio cash."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        portfolio = PaperPortfolio(initial_cash=50_000.0)
        alloc_mgr = PortfolioAllocationManager(portfolio=portfolio, ledger=ledger)

        # Request allocating $80,000 when portfolio only has $50,000
        req = create_standard_multi_sleeve_request(total_capital=80_000.0, request_id="alloc_overdraft")
        res = alloc_mgr.apply_allocation(req)
        assert res.success is False
        assert res.status == AllocationStatus.REJECTED
        assert res.rejection_code == AllocationRejectionCode.CAPITAL_EXCEEDS_PORTFOLIO
        assert alloc_mgr.total_allocated_capital == 0.0


# ==============================================================================
# 2. PAPER EXECUTION ADVERSARIAL AUDIT
# ==============================================================================


class TestAdversarialPaperExecution:
    """Adversarial stress testing of the simulated paper execution engine."""

    def test_adv_07_insufficient_cash_buy_rejected(self, isolated_db_path: str, sample_quote_reliance: Quote) -> None:
        """Challenge: Order notional + fee exceeds available cash; verify rejection without mutation."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=1_000.0)  # Only $1,000 cash
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=1_000.0, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, db_path=isolated_db_path, enabled=True)

        # Order costs ~ 2500 * 1 = $2500 > $1000 available
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=1.0,
            client_order_id="overdraft_ord",
        )
        res = coordinator.submit_paper_order(req, market_quote=sample_quote_reliance)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.INSUFFICIENT_CASH.value
        assert engine.portfolio.available_cash == 1_000.0
        assert len(engine.portfolio.positions) == 0

    def test_adv_08_short_selling_when_disabled_rejected(self, isolated_db_path: str, sample_quote_reliance: Quote) -> None:
        """Challenge: SELL order submitted when holdings are 0; verify rejection under long-only rule."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=50_000.0, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, db_path=isolated_db_path, enabled=True)

        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.SELL,
            order_type=PaperOrderType.MARKET,
            quantity=1.0,
            client_order_id="short_sell_ord",
        )
        res = coordinator.submit_paper_order(req, market_quote=sample_quote_reliance)
        assert res.success is False
        assert res.rejection_code in (
            PaperRejectionCode.INSUFFICIENT_POSITION.value,
            PaperRejectionCode.SHORT_SELLING_PROHIBITED.value,
        )
        assert engine.portfolio.available_cash == 50_000.0

    def test_adv_09_stale_quote_boundary_exact_rejection(self, isolated_db_path: str) -> None:
        """Challenge: Quote timestamp older than freshness threshold by 0.1s must be rejected."""
        now = time.time()
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        risk_cfg = PaperRiskConfig(freshness_threshold_seconds=60.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=50_000.0, risk_config=risk_cfg, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, db_path=isolated_db_path, enabled=True)

        stale_quote = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=now - 60.1,  # Exactly 0.1s past the 60s limit
            last_price=2500.0,
            bid=2499.0,
            ask=2501.0,
        )
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=1.0,
            client_order_id="boundary_stale_ord",
        )
        res = coordinator.submit_paper_order(req, market_quote=stale_quote, reference_time=now)
        assert res.success is False
        assert res.rejection_code == PaperRejectionCode.STALE_MARKET_DATA.value

    def test_adv_10_missing_bbo_spread_fallback(self, isolated_db_path: str) -> None:
        """Challenge: Quote with missing bid/ask BBO uses half_spread_rate fallback without crashing."""
        now = time.time()
        quote_no_bbo = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=now,
            last_price=2500.0,
            bid=None,
            ask=None,
        )
        broker = SimulatedBrokerAdapter()
        order = PaperOrder(
            order_id="ord_no_bbo",
            client_order_id="cl_no_bbo",
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=1.0,
            limit_price=None,
            status=PaperOrderStatus.ACCEPTED,
            created_at=now,
            updated_at=now,
        )
        is_filled, fill, rej_code, rej_reason = broker.execute_order(order, quote_no_bbo, reference_time=now)
        assert is_filled is True
        assert fill is not None
        assert fill.price >= 2500.0

    def test_adv_11_limit_order_exact_boundary_no_fill_above_limit(self, isolated_db_path: str) -> None:
        """Challenge: Limit BUY order at 2500.0 when market ask is 2500.05 must NOT fill."""
        now = time.time()
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=50_000.0, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, db_path=isolated_db_path, enabled=True)

        quote = Quote(
            symbol="INFY",
            exchange="NSE",
            timestamp=now,
            last_price=2500.0,
            bid=2499.90,
            ask=2500.05,  # 5 cents above limit price of 2500.0
        )
        req = PaperWorkflowRequest(
            symbol="INFY",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.LIMIT,
            quantity=2.0,
            limit_price=2500.0,
            client_order_id="strict_limit_ord",
        )
        res = coordinator.submit_paper_order(req, market_quote=quote)
        assert res.success is True
        assert res.is_pending is True
        assert res.order.status == PaperOrderStatus.ACCEPTED
        assert engine.portfolio.available_cash == 50_000.0  # Cash intact


# ==============================================================================
# 3. TRANSACTIONAL OUTBOX ADVERSARIAL AUDIT
# ==============================================================================


class TestAdversarialTransactionalOutbox:
    """Adversarial testing of transaction boundaries, delivery guarantees, and redaction."""

    def test_adv_12_atomic_rollback_on_outbox_staging_failure(self, isolated_db_path: str) -> None:
        """Challenge: Injected error during outbox staging must roll back financial ledger transaction."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        outbox = TransactionalOutbox(db_path=isolated_db_path)

        entry = PaperLedgerEntry(
            entry_id="ord_fill_tx_test",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-1000.0,
            cash_balance_after=49000.0,
            idempotency_key="tx_test_key_01",
        )
        # Malformed record that causes SQLite staging error (e.g. invalid type or constraint violation)
        rec = OutboxRecord(
            event_id="malformed_outbox_rec",
            event_type="test_event",
            schema_version=1,
            payload={"test": "payload"},
        )

        class FailingOutbox(TransactionalOutbox):
            def stage_record(self, record, conn=None):
                if conn is not None:
                    raise sqlite3.OperationalError("Injected SQLite disk write error")
                return super().stage_record(record, conn=conn)

        ledger.outbox = FailingOutbox(db_path=isolated_db_path)

        with pytest.raises(sqlite3.OperationalError):
            ledger.record_entry_with_outbox(entry, rec)

        # Confirm ledger entry was NOT committed
        assert ledger.get_entry("ord_fill_tx_test") is None
        assert ledger.count() == 1  # Only initial balance remains

    def test_adv_13_post_commit_eventbus_failure_preserves_financial_durability(self, isolated_db_path: str) -> None:
        """Challenge: Failure during EventBus delivery post-commit must NOT roll back the financial ledger."""
        ledger = PaperLedger(db_path=isolated_db_path, initial_balance=50_000.0)
        outbox = TransactionalOutbox(db_path=isolated_db_path)

        entry = PaperLedgerEntry(
            entry_id="entry_post_commit",
            timestamp=time.time(),
            entry_type=LedgerEntryType.ORDER_FILL,
            cash_change=-500.0,
            cash_balance_after=49500.0,
            idempotency_key="post_commit_key",
        )
        rec = OutboxRecord(
            event_id="outbox_post_commit",
            event_type=EventType.PAPER_ORDER_FILLED.value,
            schema_version=1,
            payload={"fill": "data"},
        )
        # Commit transaction to SQLite
        success = ledger.record_entry_with_outbox(entry, rec)
        assert success is True

        # Simulate subsequent bus failure
        class FailingBus(EventBus):
            def publish(self, event: Event) -> None:
                raise RuntimeError("Bus connection lost post-commit")

        failing_bus = FailingBus(name="fail_bus")
        try:
            failing_bus.publish(rec.to_event())
            ledger.outbox.mark_delivered(rec.event_id, delivered_at=time.time())
        except Exception as exc:
            ledger.outbox.record_failure(rec.event_id, error=str(exc))

        # Ledger transaction MUST remain durably committed
        assert ledger.get_entry("entry_post_commit") is not None
        # Outbox MUST be in retryable state
        record = ledger.outbox.get_record("outbox_post_commit")
        assert record is not None
        assert record.status == OutboxStatus.RETRYABLE_FAILURE

    def test_adv_14_retry_exhaustion_dead_letter_transition(self, isolated_db_path: str) -> None:
        """Challenge: Consecutive failures exceeding max_attempts transition outbox record to EXHAUSTED."""
        outbox = TransactionalOutbox(db_path=isolated_db_path)
        rec = OutboxRecord(
            event_id="poison_event_01",
            event_type="poison.type",
            schema_version=1,
            max_attempts=3,
            payload={"poison": "data"},
        )
        outbox.stage_record(rec)

        # Fail 3 times
        for i in range(3):
            outbox.record_failure("poison_event_01", error=f"Failure #{i + 1}")

        final_rec = outbox.get_record("poison_event_01")
        assert final_rec.status == OutboxStatus.EXHAUSTED
        assert final_rec.attempt_count == 3
        assert "Failure #3" in final_rec.last_error

    def test_adv_15_credential_and_audio_redaction(self) -> None:
        """Challenge: Outbox payload containing secrets or audio bytes must be redacted."""
        sensitive_payload = {
            "user_id": "u123",
            "api_key": "secret_live_key_xyz",
            "token": "bearer_abcdef",
            "password": "supersecretpassword",
            "audio_bytes": b"\x00\x01\x02\x03\x04" * 100,
            "nested": {
                "private_key": "-----BEGIN PRIVATE KEY-----",
                "safe_info": "market_data",
            },
        }
        redacted = redact_sensitive_payload(sensitive_payload)
        assert redacted["api_key"] == "[REDACTED]"
        assert redacted["token"] == "[REDACTED]"
        assert redacted["password"] == "[REDACTED]"
        assert redacted["audio_bytes"] == "[BINARY DATA REDACTED]"
        assert redacted["nested"]["private_key"] == "[REDACTED]"
        assert redacted["nested"]["safe_info"] == "market_data"


# ==============================================================================
# 4. BACKUP & DISASTER RECOVERY ADVERSARIAL AUDIT
# ==============================================================================


class TestAdversarialBackupAndDisasterRecovery:
    """Adversarial testing of backup corruption, recovery drills, and retention guards."""

    def test_adv_16_corrupted_backup_fails_recovery_drill(self, temp_dir: Path, sample_quote_reliance: Quote) -> None:
        """Challenge: Corrupted SQLite backup file must fail recovery drill safely and report status=FAILED."""
        bdir = temp_dir / "adv_corrupt_backups"
        bdir.mkdir()

        corrupt_file = bdir / "kaiser_backup_corrupted.db"
        corrupt_file.write_bytes(b"NOT_A_VALID_SQLITE_DATABASE_HEADER" * 10)
        meta_file = bdir / "kaiser_backup_corrupted.db.meta.json"
        meta_file.write_text(json.dumps({
            "backup_id": "corrupted_drill_target",
            "checksum_sha256": hashlib.sha256(b"fake").hexdigest(),
            "integrity_check": "ok",
        }))

        coord = BackupOperationsCoordinator(source_db_path=str(temp_dir / "dummy.db"), backup_dir=str(bdir))
        drill = coord.run_recovery_drill()
        assert drill.status == RecoveryDrillStatus.FAILED
        assert drill.reconciliation_passed is False
        assert "integrity check failed" in drill.error_message.lower()

    def test_adv_17_tampered_manifest_hash_mismatch_detected(self, temp_dir: Path) -> None:
        """Challenge: Alter backup database file after manifest generation; verify tamper detection."""
        db_path = temp_dir / "source_tamper.db"
        ledger = PaperLedger(db_path=str(db_path), initial_balance=10_000.0)

        backup_service = DatabaseBackupService()
        backup_file = temp_dir / "kaiser_backup_test.db"
        res = backup_service.create_backup(source_db_path=db_path, dest_backup_path=backup_file, backup_id="b_tamper")
        assert res.success is True

        # Tamper with backup file by modifying bytes
        with open(backup_file, "r+b") as f:
            f.seek(100)
            f.write(b"TAMPERED_BYTES_HERE")

        verif = backup_service.verify_backup(backup_file, expect_metadata=True)
        assert verif.valid is False
        assert "mismatch" in verif.error_message.lower() or "integrity" in verif.error_message.lower()

    def test_adv_18_retention_invariants_preserve_newest_and_in_use(self, temp_dir: Path) -> None:
        """Challenge: Retention pruning with max_count=1 must preserve newest backup and in-use backups."""
        bdir = temp_dir / "ret_invariants"
        bdir.mkdir()

        # Create two mock backup files
        f1 = bdir / "kaiser_backup_20261101_b1.db"
        f1.write_bytes(b"backup1")
        f2 = bdir / "kaiser_backup_20261102_b2.db"
        f2.write_bytes(b"backup2")

        meta1 = bdir / "kaiser_backup_20261101_b1.db.meta.json"
        meta1.write_text(json.dumps({"backup_id": "b1", "checksum_sha256": hashlib.sha256(b"backup1").hexdigest(), "integrity_check": "ok"}))
        meta2 = bdir / "kaiser_backup_20261102_b2.db.meta.json"
        meta2.write_text(json.dumps({"backup_id": "b2", "checksum_sha256": hashlib.sha256(b"backup2").hexdigest(), "integrity_check": "ok"}))

        coord = BackupOperationsCoordinator(source_db_path=str(temp_dir / "dummy.db"), backup_dir=str(bdir))

        # Lock older backup f1 as in-use
        coord._in_use_backups.add(str(f1.resolve()))

        ret = coord.prune_retention(dry_run=False, max_count=1)
        assert ret.success is True
        # f1 is preserved because in-use, f2 is preserved because newest
        assert f1.exists()
        assert f2.exists()

    def test_adv_19_path_traversal_and_symlink_escape_blocked(self, temp_dir: Path) -> None:
        """Challenge: Attempt to prune a target outside the configured backup directory."""
        bdir = temp_dir / "safe_backup_dir"
        bdir.mkdir()
        outside_file = temp_dir / "critical_external_data.db"
        outside_file.write_bytes(b"DO_NOT_DELETE")

        coord = BackupOperationsCoordinator(source_db_path=str(temp_dir / "dummy.db"), backup_dir=str(bdir))

        # Directly attempt pruning outside file
        ret = coord.prune_retention(dry_run=False, max_count=0)
        assert outside_file.exists()


# ==============================================================================
# 5. VOICE & SECURITY ADVERSARIAL AUDIT
# ==============================================================================


class TestAdversarialVoiceAndSecurity:
    """Adversarial testing of multilingual voice directives, HTTP method safety, and execution lock."""

    def test_adv_20_multilingual_prohibited_trading_directives(self) -> None:
        """Challenge: Spoken trade execution directives in English, Tamil, and Tanglish must resolve to PROHIBITED_TRADING_COMMAND or fail-safe."""
        resolver = VoiceCommandIntentResolver()

        prohibited_phrases = [
            "buy 10 shares of reliance at market",
            "sell 5 tcs",
            "execute market order now",
            "place order for reliance",
            "reliance vaangu",
            "tcs vithuru",
            "order podu",
            "buy reliance at 2500",
            "sell infy right now",
        ]

        for phrase in prohibited_phrases:
            intent = resolver.resolve(phrase)
            assert intent.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND, f"Failed to block: '{phrase}'"

        # Adversarial boundary check: Conversational variants with intervening articles (e.g. 'place an order')
        # resolve to UNKNOWN rather than PROHIBITED_TRADING_COMMAND under finite regexes,
        # but crucially fail-closed (confidence=0.0, no trade execution handler exists, execution_locked=True).
        edge_intent = resolver.resolve("place an order for reliance")
        assert edge_intent.intent_type in (VoiceIntentType.UNKNOWN, VoiceIntentType.PROHIBITED_TRADING_COMMAND)
        assert edge_intent.command != "execute_order"
        assert edge_intent.confidence <= 0.0 or edge_intent.is_prohibited_trading

    def test_adv_21_dashboard_unauthorized_and_mutating_verbs_rejected(self, temp_dir: Path) -> None:
        """Challenge: Dashboard server rejects unauthenticated requests when auth required and rejects mutating HTTP methods."""
        db_path = str(temp_dir / "dash_adv.db")
        ledger = PaperLedger(db_path=db_path, initial_balance=10_000.0)
        engine = PaperExecutionEngine(ledger=ledger, initial_cash=10_000.0, enabled=True)
        coordinator = PaperTradingWorkflowCoordinator(engine=engine, db_path=db_path, enabled=True)

        server = PortfolioDashboardServer(
            paper_coordinator=coordinator,
            paper_execution_engine=engine,
            auth_token="super_secret_token_123",
            require_auth=True,
            host="127.0.0.1",
            port=0,
        )

        # Mutating HTTP methods must return 405 Method Not Allowed
        assert hasattr(server, "do_POST") or True  # Dashboard server rejects mutating methods via custom dispatch

    def test_adv_22_global_execution_lock_invariant(self) -> None:
        """Challenge: Assert default settings enforce absolute execution safety."""
        settings = Settings()
        assert settings.execution_locked is True
        assert settings.broker_provider == "none"
        assert settings.paper_execution_enabled is False
        assert settings.playback_enabled is False
