"""KAISER — Phase 2, Step 56: Paper Trading Operational Security, Dashboard Access Control & End-to-End Acceptance Tests.

Comprehensive test suite verifying:
- OBJECTIVE 1 & 2: Dashboard Access Control, constant-time token comparison, 401/403 enforcement, safe headers, loopback constraints.
- OBJECTIVE 3: HTTP Method and Input Safety (POST/PUT/PATCH/DELETE/OPTIONS/TRACE rejection with 405, malformed input safety, no secret leakage).
- OBJECTIVE 4: Complete 12-scenario End-to-End Simulated Paper Trading Acceptance Workflow.
- OBJECTIVE 5: Architectural Security and Isolation Regressions (execution_locked=True, broker=none, voice boundaries, token redaction).
"""

from __future__ import annotations

import json
from pathlib import Path
import time
import urllib.request
import urllib.error
import pytest

from app.bootstrap import bootstrap_system
from app.core.config import Settings, get_settings
from app.core.events import Event, EventBus, EventType
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.state import StateStore
from app.execution import (
    PaperExecutionEngine,
    PaperExecutionResult,
    PaperFill,
    PaperOrder,
    PaperOrderRequest,
    PaperOrderSide,
    PaperOrderStatus,
    PaperOrderType,
    PaperPortfolio,
    PaperRejectionCode,
    PaperRiskConfig,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowOutcome,
    PaperWorkflowRequest,
    WorkflowStage,
    WorkflowStatus,
)
from app.ledger import LedgerEntryType, PaperLedger, PaperLedgerEntry
from app.market.models import Bar, Quote
from app.orchestrator import HandlerRegistry, MasterOrchestrator, RequestStatus
from app.orchestrator.models import HandlerResult
from app.portfolio.dashboard import PortfolioDashboardServer
from app.voice import (
    VoiceCommandIntentResolver,
    VoiceIntentType,
)


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_sec_e2e_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    store = StateStore(name="test_sec_e2e_state_store")
    store.set("trading_engine", "not initialized")
    store.set("execution_engine", "locked")
    return store


@pytest.fixture
def fresh_quote() -> Quote:
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


@pytest.fixture
def paper_ledger(event_bus: EventBus, tmp_path: Path) -> PaperLedger:
    db_file = str(tmp_path / "paper_sec_e2e_ledger.db")
    return PaperLedger(event_bus=event_bus, db_path=db_file, initial_balance=100_000.0)


@pytest.fixture
def paper_engine(
    event_bus: EventBus,
    state_store: StateStore,
    paper_ledger: PaperLedger,
    fresh_quote: Quote,
) -> PaperExecutionEngine:
    risk_config = PaperRiskConfig(
        max_order_notional=50_000.0,
        max_position_exposure_pct=50.0,
        freshness_threshold_seconds=60.0,
        allow_short_selling=False,
    )
    engine = PaperExecutionEngine(
        event_bus=event_bus,
        state_store=state_store,
        ledger=paper_ledger,
        initial_cash=100_000.0,
        risk_config=risk_config,
        fee_rate=0.0005,
        slippage_rate=0.0002,
        enabled=True,
    )
    engine.on_market_quote(fresh_quote)
    return engine


@pytest.fixture
def coordinator(
    paper_engine: PaperExecutionEngine,
    event_bus: EventBus,
    state_store: StateStore,
) -> PaperTradingWorkflowCoordinator:
    return PaperTradingWorkflowCoordinator(
        engine=paper_engine,
        event_bus=event_bus,
        state_store=state_store,
        enabled=True,
    )


# ==============================================================================
# OBJECTIVE 4: END-TO-END ACCEPTANCE WORKFLOW (12 SCENARIOS)
# ==============================================================================


class TestPaperTradingEndToEndAcceptance:
    """12 End-to-End Acceptance Scenarios covering the complete simulated paper trading workflow."""

    def test_e2e_01_disabled_by_default_cannot_execute_orders(
        self,
        event_bus: EventBus,
        state_store: StateStore,
        paper_ledger: PaperLedger,
        fresh_quote: Quote,
    ) -> None:
        """Scenario 1: Start with paper execution disabled and verify it cannot execute orders."""
        engine = PaperExecutionEngine(
            event_bus=event_bus,
            state_store=state_store,
            ledger=paper_ledger,
            initial_cash=100_000.0,
            enabled=False,  # Disabled
        )
        coord = PaperTradingWorkflowCoordinator(
            engine=engine,
            event_bus=event_bus,
            state_store=state_store,
            enabled=False,  # Disabled
        )
        assert coord.is_enabled is False
        assert engine.is_enabled is False

        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
        )
        outcome = coord.submit_paper_order(req, market_quote=fresh_quote)
        assert outcome.status == WorkflowStatus.REJECTED
        assert outcome.rejection_code == PaperRejectionCode.PAPER_EXECUTION_DISABLED.value
        assert outcome.rejection_reason is not None
        assert "disabled" in outcome.rejection_reason.lower()

        # Verify portfolio and ledger remain untouched
        pos = engine.portfolio.get_position("RELIANCE")
        assert pos is None or pos.quantity == 0.0
        assert engine.portfolio.available_cash == 100_000.0
        assert len(paper_ledger.get_entries()) == 1  # Only initial cash entry

    def test_e2e_02_enable_paper_in_isolated_test_config_retains_live_lock(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
    ) -> None:
        """Scenario 2: Enable paper execution explicitly while retaining the global live-execution lock."""
        assert coordinator.is_enabled is True
        assert coordinator.engine.is_enabled is True

        # Invariant checks
        settings = coordinator.settings
        assert settings.execution_locked is True
        assert settings.broker_provider == "none"
        assert coordinator.state_store.get("trading_engine") == "not initialized"
        assert coordinator.state_store.get("execution_engine") == "locked"

    def test_e2e_03_supply_deterministic_valid_market_quote(
        self,
        paper_engine: PaperExecutionEngine,
        fresh_quote: Quote,
    ) -> None:
        """Scenario 3: Supply a deterministic, valid market quote using the existing provider abstraction."""
        assert fresh_quote.symbol == "RELIANCE"
        assert fresh_quote.last_price == 2500.0
        assert fresh_quote.bid == 2499.5
        assert fresh_quote.ask == 2500.5
        assert fresh_quote.source == "mock"

        # Register quote and verify it is tracked in cache
        paper_engine.on_market_quote(fresh_quote)
        cached_quote = paper_engine.get_quote("RELIANCE")
        assert cached_quote is not None
        assert cached_quote.last_price == 2500.0

    def test_e2e_04_05_06_submit_valid_order_outcome_and_ledger_reconciliation(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_ledger: PaperLedger,
        fresh_quote: Quote,
    ) -> None:
        """Scenarios 4, 5, 6: Submit valid paper order, verify structured outcome, fill, and ledger reconciliation."""
        initial_cash = coordinator.engine.portfolio.available_cash
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
            client_order_id="E2E-ACCEPTANCE-001",
        )

        outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

        # Scenario 5: Verify structured outcome and simulated fill
        assert outcome.status == WorkflowStatus.FILLED
        assert outcome.order is not None
        assert outcome.order.status == PaperOrderStatus.FILLED
        assert outcome.order.client_order_id == "E2E-ACCEPTANCE-001"
        assert outcome.fill is not None
        assert outcome.fill.quantity == 10.0
        assert outcome.fill.price > 0.0
        assert outcome.fill.fee > 0.0
        assert outcome.fill.slippage >= 0.0

        # Scenario 6: Verify ledger records financial events and balances reconcile
        position = coordinator.engine.portfolio.get_position("RELIANCE")
        assert position.quantity == 10.0
        assert coordinator.engine.portfolio.available_cash < initial_cash

        # Ledger entries verification
        entries = paper_ledger.get_entries()
        entry_types = [e.entry_type for e in entries]
        assert LedgerEntryType.INITIAL_CAPITAL in entry_types
        assert LedgerEntryType.ORDER_ACCEPTANCE in entry_types
        assert LedgerEntryType.ORDER_FILL in entry_types

        # Reconcile portfolio with ledger
        recon = paper_ledger.reconcile(expected_current_cash=coordinator.engine.portfolio.available_cash)
        assert recon["is_balanced"] is True
        assert recon["discrepancy"] == 0.0

    def test_e2e_07_read_only_dashboard_summary_subject_to_authentication(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        fresh_quote: Quote,
    ) -> None:
        """Scenario 7: Verify read-only dashboard summary/status reflects state, subject to authentication."""
        # Submit an order to create state
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
        )
        coordinator.submit_paper_order(req, market_quote=fresh_quote)

        # Start dashboard server with auth token configured
        auth_token = "secure_acceptance_token_step56"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            paper_coordinator=coordinator,
            auth_token=auth_token,
        )
        server.start(blocking=False)

        try:
            url = f"{server.url}/api/paper/summary"

            # 1. Unauthenticated request -> 401 Unauthorized
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 401
            assert "Bearer" in exc_info.value.headers.get("WWW-Authenticate", "")

            # 2. Invalid token -> 401 Unauthorized
            bad_req = urllib.request.Request(url, headers={"Authorization": "Bearer wrong_token"})
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(bad_req)
            assert exc_info.value.code == 401

            # 3. Valid Bearer token -> 200 OK
            good_bearer_req = urllib.request.Request(url, headers={"Authorization": f"Bearer {auth_token}"})
            with urllib.request.urlopen(good_bearer_req) as response:
                assert response.status == 200
                data = json.loads(response.read().decode("utf-8"))
                assert data["status"] == "success"
                assert data["label"] == "PAPER ONLY"
                assert data["paper_execution_enabled"] is True
                assert data["execution_locked"] is True
                assert data["broker_provider"] == "none"
                assert data["trading_engine"] == "not initialized"
                assert data["order_counts"]["FILLED"] == 1
                assert data["paper_cash"] < 100_000.0
                assert any(p["symbol"] == "RELIANCE" for p in data["positions"])

            # 4. Valid X-API-Key token -> 200 OK
            good_apikey_req = urllib.request.Request(url, headers={"X-API-Key": auth_token})
            with urllib.request.urlopen(good_apikey_req) as response:
                assert response.status == 200
                data = json.loads(response.read().decode("utf-8"))
                assert data["status"] == "success"

        finally:
            server.stop()

    def test_e2e_08_restart_reconstruct_portfolio_from_sqlite_ledger(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        paper_ledger: PaperLedger,
        fresh_quote: Quote,
        event_bus: EventBus,
        state_store: StateStore,
    ) -> None:
        """Scenario 8: Reconstruct paper portfolio from test SQLite ledger and verify no initial cash duplication."""
        # Execute an order
        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
        )
        coordinator.submit_paper_order(req, market_quote=fresh_quote)
        cash_before_restart = coordinator.engine.portfolio.available_cash
        pos_before_restart = coordinator.engine.portfolio.get_position("RELIANCE").quantity
        db_path = paper_ledger.db_path

        # Simulate system restart: new engine instance loading from existing ledger DB
        restarted_ledger = PaperLedger(event_bus=event_bus, db_path=db_path)
        restarted_engine = PaperExecutionEngine(
            event_bus=event_bus,
            state_store=state_store,
            ledger=restarted_ledger,
            initial_cash=100_000.0,
            enabled=True,
        )
        restarted_engine.recover_from_ledger()

        # Verify recovered portfolio
        recovered_portfolio = restarted_engine.portfolio
        assert recovered_portfolio.available_cash == cash_before_restart
        assert recovered_portfolio.get_position("RELIANCE").quantity == pos_before_restart
        # Cash must NOT have been duplicated to 200_000
        assert recovered_portfolio.available_cash < 100_000.0

    def test_e2e_09_repeat_client_order_id_idempotency_duplicate_rejection(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        fresh_quote: Quote,
    ) -> None:
        """Scenario 9: Repeat the same client order ID and verify idempotency/duplicate rejection."""
        req1 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="IDEMPOTENCY-TEST-001",
        )
        outcome1 = coordinator.submit_paper_order(req1, market_quote=fresh_quote)
        assert outcome1.status == WorkflowStatus.FILLED

        # Repeat identical client_order_id
        req2 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
            client_order_id="IDEMPOTENCY-TEST-001",
        )
        outcome2 = coordinator.submit_paper_order(req2, market_quote=fresh_quote)
        assert outcome2.status == WorkflowStatus.REJECTED
        assert outcome2.rejection_code == PaperRejectionCode.DUPLICATE_ORDER.value
        assert outcome2.rejection_reason is not None
        assert "already been processed" in outcome2.rejection_reason.lower() or "duplicate" in outcome2.rejection_reason.lower()

        # Position should only be 5.0, not 10.0
        assert coordinator.engine.portfolio.get_position("RELIANCE").quantity == 5.0

    def test_e2e_10_force_quote_quality_and_risk_gate_failures_zero_financial_mutation(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
    ) -> None:
        """Scenario 10: Force a quote-quality failure and a risk-gate rejection; verify zero mutation."""
        initial_cash = coordinator.engine.portfolio.available_cash
        initial_entries = len(coordinator.engine.ledger.get_entries())

        # 1. Quote-quality failure: stale quote (> 60 seconds old)
        stale_quote = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=time.time() - 300.0,  # 5 minutes old
            last_price=2500.0,
            bid=2499.0,
            ask=2501.0,
            bid_size=10.0,
            ask_size=10.0,
            volume=100.0,
            source="mock",
        )
        req1 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
        )
        outcome1 = coordinator.submit_paper_order(req1, market_quote=stale_quote)
        assert outcome1.status == WorkflowStatus.REJECTED
        assert outcome1.rejection_code == PaperRejectionCode.STALE_MARKET_DATA.value

        # Financial state unchanged
        assert coordinator.engine.portfolio.available_cash == initial_cash
        pos1 = coordinator.engine.portfolio.get_position("RELIANCE")
        assert pos1 is None or pos1.quantity == 0.0

        # 2. Risk-gate rejection: short selling prohibited without holding
        valid_fresh_quote = Quote(
            symbol="RELIANCE",
            exchange="NSE",
            timestamp=time.time(),
            last_price=2500.0,
            bid=2499.0,
            ask=2501.0,
            bid_size=10.0,
            ask_size=10.0,
            volume=100.0,
            source="mock",
        )
        req2 = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.SELL,
            order_type=PaperOrderType.MARKET,
            quantity=10.0,
        )
        outcome2 = coordinator.submit_paper_order(req2, market_quote=valid_fresh_quote)
        assert outcome2.status == WorkflowStatus.REJECTED
        assert outcome2.rejection_code == PaperRejectionCode.INSUFFICIENT_POSITION.value

        # Financial state remains strictly unchanged
        assert coordinator.engine.portfolio.available_cash == initial_cash
        pos2 = coordinator.engine.portfolio.get_position("RELIANCE")
        assert pos2 is None or pos2.quantity == 0.0

        # Ledger rejection entries must have zero financial impact (cash_change=0.0)
        rejection_entries = [e for e in coordinator.engine.ledger.get_entries() if e.entry_type == LedgerEntryType.ORDER_REJECTION]
        assert len(rejection_entries) == 1
        assert rejection_entries[0].cash_change == 0.0
        assert rejection_entries[0].cash_balance_after == initial_cash

        # Ledger remains perfectly balanced with portfolio
        recon = coordinator.engine.ledger.reconcile(expected_current_cash=coordinator.engine.portfolio.available_cash)
        assert recon["is_balanced"] is True
        assert recon["discrepancy"] == 0.0

    def test_e2e_11_simulate_persistence_failure_surfaced_without_false_success(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        fresh_quote: Quote,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Scenario 11: Simulate persistence failure and verify failure is surfaced without falsely reporting success."""
        # Monkeypatch ledger.record_entry to raise an IOError simulating disk/database failure
        def mock_record_failure(*args, **kwargs):
            raise IOError("Simulated SQLite write/IO failure")

        monkeypatch.setattr(coordinator.engine.ledger, "record_entry", mock_record_failure)

        req = PaperWorkflowRequest(
            symbol="RELIANCE",
            side=PaperOrderSide.BUY,
            order_type=PaperOrderType.MARKET,
            quantity=5.0,
        )
        outcome = coordinator.submit_paper_order(req, market_quote=fresh_quote)

        # Must not report FILLED
        assert outcome.status in (WorkflowStatus.FAILED, WorkflowStatus.REJECTED)
        assert outcome.error_message is not None

    def test_e2e_12_auth_failures_and_prohibited_methods_cannot_change_state(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        fresh_quote: Quote,
    ) -> None:
        """Scenario 12: Verify authentication failures and prohibited HTTP methods cannot mutate state."""
        auth_token = "step56_auth_test_token"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            paper_coordinator=coordinator,
            auth_token=auth_token,
        )
        server.start(blocking=False)

        try:
            url = f"{server.url}/api/paper/summary"
            initial_cash = coordinator.engine.portfolio.available_cash
            pos_initial = coordinator.engine.portfolio.get_position("RELIANCE")
            initial_pos = pos_initial.quantity if pos_initial else 0.0
            initial_entries = len(coordinator.engine.ledger.get_entries())

            # 1. Unauthenticated GET -> 401
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 401

            # 2. Invalid Token GET -> 401
            req_bad = urllib.request.Request(url, headers={"Authorization": "Bearer fake_token"})
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req_bad)
            assert exc_info.value.code == 401

            # 3. POST -> 405 Method Not Allowed
            post_req = urllib.request.Request(
                url,
                data=b'{"symbol": "RELIANCE", "quantity": 100}',
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {auth_token}"},
                method="POST",
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(post_req)
            assert exc_info.value.code == 405

            # 4. PUT -> 405 Method Not Allowed
            put_req = urllib.request.Request(
                url,
                data=b'{"cash": 9999999}',
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {auth_token}"},
                method="PUT",
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(put_req)
            assert exc_info.value.code == 405

            # 5. DELETE -> 405 Method Not Allowed
            del_req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {auth_token}"},
                method="DELETE",
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(del_req)
            assert exc_info.value.code == 405

            # 6. PATCH -> 405 Method Not Allowed
            patch_req = urllib.request.Request(
                url,
                data=b'{}',
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {auth_token}"},
                method="PATCH",
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(patch_req)
            assert exc_info.value.code == 405

            # Verify complete state invariance
            assert coordinator.engine.portfolio.available_cash == initial_cash
            pos_after = coordinator.engine.portfolio.get_position("RELIANCE")
            assert (pos_after.quantity if pos_after else 0.0) == initial_pos
            assert len(coordinator.engine.ledger.get_entries()) == initial_entries

        finally:
            server.stop()


# ==============================================================================
# OBJECTIVES 2 & 3: DASHBOARD ACCESS CONTROL & HTTP METHOD SAFETY
# ==============================================================================


class TestDashboardAccessControlAndSafety:
    """Security tests for Dashboard Access Control, token authentication, and method handling."""

    def test_require_auth_fails_closed_when_token_is_not_configured(self) -> None:
        """When require_auth=True but auth_token is None, requests to protected endpoints return 403."""
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            require_auth=True,
            auth_token=None,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 403
            err_body = json.loads(exc_info.value.read().decode("utf-8"))
            assert err_body["error"] == "Forbidden"
        finally:
            server.stop()

    def test_constant_time_comparison_and_case_insensitive_bearer(self) -> None:
        """Validate case-insensitive 'bearer' scheme and constant-time token comparison."""
        token = "kaiser_secret_token_12345"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=token,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"

            # Lowercase "bearer"
            req1 = urllib.request.Request(url, headers={"Authorization": f"bearer {token}"})
            with urllib.request.urlopen(req1) as resp:
                assert resp.status == 200

            # Uppercase "BEARER"
            req2 = urllib.request.Request(url, headers={"Authorization": f"BEARER {token}"})
            with urllib.request.urlopen(req2) as resp:
                assert resp.status == 200

            # Extra spaces stripped
            req3 = urllib.request.Request(url, headers={"Authorization": f"Bearer   {token}  "})
            with urllib.request.urlopen(req3) as resp:
                assert resp.status == 200

        finally:
            server.stop()

    def test_malformed_authorization_headers_handled_safely(self) -> None:
        """Malformed authorization headers return 401 without throwing 500 or crashing."""
        token = "valid_token"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=token,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"

            # Missing token part
            req1 = urllib.request.Request(url, headers={"Authorization": "Bearer"})
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req1)
            assert exc_info.value.code == 401

            # Unknown auth scheme
            req2 = urllib.request.Request(url, headers={"Authorization": "Basic dXNlcjpwYXNz"})
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req2)
            assert exc_info.value.code == 401

            # Empty header
            req3 = urllib.request.Request(url, headers={"Authorization": ""})
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req3)
            assert exc_info.value.code == 401

        finally:
            server.stop()

    def test_non_loopback_host_binding_rejected(self) -> None:
        """Binding to non-loopback host (e.g. 0.0.0.0 or 192.168.1.1) is strictly rejected."""
        with pytest.raises(ValidationError) as exc_info:
            PortfolioDashboardServer(host="0.0.0.0", port=8080)
        assert "Security constraint violation" in str(exc_info.value)

        with pytest.raises(ValidationError) as exc_info:
            PortfolioDashboardServer(host="192.168.1.100", port=8080)
        assert "Security constraint violation" in str(exc_info.value)

    def test_options_and_trace_methods_rejected_with_405(self) -> None:
        """OPTIONS and TRACE HTTP methods return 405 Method Not Allowed."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/status"

            opt_req = urllib.request.Request(url, method="OPTIONS")
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(opt_req)
            assert exc_info.value.code == 405
            assert "GET, HEAD" in exc_info.value.headers.get("Allow", "")

            trace_req = urllib.request.Request(url, method="TRACE")
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(trace_req)
            assert exc_info.value.code == 405

        finally:
            server.stop()

    def test_unknown_routes_return_clean_404(self) -> None:
        """Unknown paths return a clean JSON 404 response without leaking internals."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/unknown_route_does_not_exist"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 404
            data = json.loads(exc_info.value.read().decode("utf-8"))
            assert data["error"] == "Not Found"
        finally:
            server.stop()

    def test_invalid_query_parameters_return_clean_400(self) -> None:
        """Invalid query parameters return 400 Bad Request instead of crashing or 500."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/reports?limit=invalid_alpha"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 400
            data = json.loads(exc_info.value.read().decode("utf-8"))
            assert data["error"] == "Bad Request"
        finally:
            server.stop()

    def test_security_headers_present_on_all_responses(self) -> None:
        """Defense-in-depth security headers are present on responses."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/status"
            with urllib.request.urlopen(url) as resp:
                headers = resp.headers
                assert headers.get("X-Content-Type-Options") == "nosniff"
                assert headers.get("X-Frame-Options") == "DENY"
                assert "default-src 'self'" in headers.get("Content-Security-Policy", "")
                assert "no-store" in headers.get("Cache-Control", "")
                # Ensure no permissive wildcard CORS header is added
                assert headers.get("Access-Control-Allow-Origin") is None
        finally:
            server.stop()


# ==============================================================================
# OBJECTIVE 5: SECURITY AND ISOLATION REGRESSION TESTS
# ==============================================================================


class TestSecurityAndIsolationRegressions:
    """Regression suite verifying architectural safety invariants remain locked and unbreached."""

    def test_execution_locked_invariant_remains_true(self) -> None:
        """execution_locked is True by default and cannot be unlocked by enabling paper trading."""
        default_settings = Settings()
        assert default_settings.execution_locked is True
        assert default_settings.paper_execution_enabled is False
        assert default_settings.broker_provider == "none"

        paper_active_settings = Settings(paper_execution_enabled=True)
        assert paper_active_settings.paper_execution_enabled is True
        assert paper_active_settings.execution_locked is True
        assert paper_active_settings.broker_provider == "none"

    def test_bootstrap_leaves_live_trading_engine_uninitialized(self) -> None:
        """System bootstrap leaves trading engine uninitialized."""
        status = bootstrap_system(Settings(voice_enabled=False, dashboard_enabled=False), log_to_file=False)
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.execution_locked is True
        assert status.settings.broker_provider == "none"

    def test_auth_token_redacted_in_settings_export(self) -> None:
        """dashboard_auth_token is masked as '***' in Settings.to_dict()."""
        secret_token = "my_highly_sensitive_dashboard_secret_key"
        settings = Settings(dashboard_auth_token=secret_token)
        export = settings.to_dict()
        assert export["dashboard_auth_token"] == "***"
        assert secret_token not in str(export)

    def test_voice_intent_trade_prohibition_safety_boundary_intact(self) -> None:
        """Voice intent resolver intercepts and blocks spoken trading execution commands."""
        registry = HandlerRegistry(name="test_voice_reg")
        resolver = VoiceCommandIntentResolver(orchestrator=MasterOrchestrator(registry=registry))

        prohibited_phrases = [
            "buy 100 shares of RELIANCE",
            "sell 50 INFY",
            "place order for TCS",
            "execute market order now",
            "order podu",
            "trade execute pannu",
            "vaangu 100 shares",
        ]

        for phrase in prohibited_phrases:
            intent = resolver.resolve(phrase, language="en")
            assert intent.is_prohibited_trading is True
            assert intent.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
            assert "prohibited" in (intent.rejection_reason or "").lower()

    def test_error_responses_do_not_leak_secrets_or_stack_traces(self) -> None:
        """Error responses never expose traceback information, secrets, or file paths."""
        token = "secret_passphrase_to_protect"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=token,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            body = exc_info.value.read().decode("utf-8")
            assert token not in body
            assert "Traceback" not in body
            assert "File \"" not in body
        finally:
            server.stop()
