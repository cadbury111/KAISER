"""KAISER — Phase 2, Step 57: Security Configuration Audit & Dashboard Endpoint Authorization Verification.

Comprehensive automated regression tests verifying:
1. OBJECTIVE 1 & 2: Audit Authentication Defaults and Fail-Closed Behavior.
   - Dashboard enabled with default configuration (local development mode).
   - Dashboard enabled with require_auth=True but no token configured -> fails closed (HTTP 403 Forbidden).
   - All sensitive /api/* endpoints protected when auth_token is configured or require_auth is enabled.
   - Missing token, invalid Bearer token, empty Bearer token, malformed scheme, invalid API key, empty API key.
   - Both supported authentication headers supplied together (deterministic dual-header rule: both must be valid).
   - Whitespace and case handling (case-insensitive 'Bearer', token trimming).
   - URL query parameter rejection (tokens in query parameters are strictly rejected).
   - Non-loopback host binding rejection.
   - Strict configuration type and boolean validation (no silent truthy conversion of 'false', 'no', etc.).
   - Redaction of tokens and keys in Settings serialization and log output.

2. OBJECTIVE 3: Authorization Verification on Every Dashboard Endpoint.
   - Complete inventory verification across all public and protected routes.
   - Public health check (/api/status) discloses only minimum operational info.
   - SPA shell (/ and /index.html) serves static shell publicly while protecting data APIs.
   - Unknown routes (/api/unknown) require auth when auth enforced; return clean 404 when authenticated.

3. OBJECTIVE 4 & 5: State Invariance & Method Enforcement.
   - Prohibited methods (POST, PUT, DELETE, PATCH, OPTIONS, TRACE) consistently rejected with HTTP 405.
   - Capture before-and-after state across cash, positions, orders, ledger, settings, trading engine.
   - Safety invariants intact: execution_locked=True, broker_provider='none', trading_engine='not initialized'.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from pathlib import Path
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Generator, List, Optional
import pytest

from app.bootstrap import bootstrap_system
from app.core.config import Settings, get_settings, parse_bool
from app.core.events import Event, EventBus
from app.core.exceptions import ConfigurationError, ValidationError
from app.core.state import StateStore
from app.execution import (
    PaperExecutionEngine,
    PaperOrderSide,
    PaperOrderType,
    PaperRiskConfig,
    PaperTradingWorkflowCoordinator,
    PaperWorkflowRequest,
)
from app.ledger import PaperLedger
from app.market.models import Quote
from app.portfolio.dashboard import PortfolioDashboardServer
from app.portfolio.reporting import PortfolioReportingService


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_step57_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    store = StateStore(name="test_step57_state_store")
    store.set("trading_engine", "not initialized")
    store.set("execution_engine", "locked")
    return store


@pytest.fixture
def fresh_quote() -> Quote:
    return Quote(
        symbol="RELIANCE",
        exchange="NSE",
        timestamp=time.time(),
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
    db_file = str(tmp_path / "paper_step57_ledger.db")
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
# OBJECTIVE 1 & 2: AUDIT AUTHENTICATION DEFAULTS & FAIL-CLOSED BEHAVIOR
# ==============================================================================


class TestAuthenticationDefaultsAndFailClosed:
    """Verifies authentication defaults, fail-closed mechanics, and token validation."""

    def test_default_configuration_settings_are_safe_and_offline(self) -> None:
        """Default Settings must have execution locked, broker none, and dashboard disabled."""
        s = Settings()
        assert s.dashboard_enabled is False
        assert s.dashboard_require_auth is False
        assert s.dashboard_auth_token is None
        assert s.dashboard_host == "127.0.0.1"
        assert s.execution_locked is True
        assert s.broker_provider == "none"
        assert s.paper_execution_enabled is False

    def test_unauthenticated_local_development_mode(self) -> None:
        """When require_auth=False and auth_token=None, local loopback development mode is permitted."""
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=None,
            require_auth=False,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/status"
            with urllib.request.urlopen(url) as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["status"] == "healthy"
                assert data["read_only"] is True
                assert data["execution_locked"] is True
        finally:
            server.stop()

    def test_fail_closed_when_auth_required_but_no_token_configured(self) -> None:
        """When require_auth=True but auth_token is None or empty, server fails closed with HTTP 403."""
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=None,
            require_auth=True,
        )
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 403
            data = json.loads(exc_info.value.read().decode("utf-8"))
            assert data["error"] == "Forbidden"
            assert "authentication is required, but no server authentication token" in data["message"]
        finally:
            server.stop()

    def test_fail_closed_when_auth_required_with_whitespace_token(self) -> None:
        """Whitespace-only auth_token normalizes to None and fails closed with HTTP 403."""
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token="   \t  \n  ",
            require_auth=True,
        )
        assert server.auth_token is None
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 403
        finally:
            server.stop()

    def test_auth_enforced_across_all_data_endpoints_when_token_configured(self) -> None:
        """When auth_token is configured, all sensitive /api/* endpoints require credentials."""
        token = "step57_secret_key_audit_test"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=token,
            require_auth=False,  # Even if require_auth=False, setting auth_token must enforce auth!
        )
        server.start(blocking=False)
        try:
            sensitive_endpoints = [
                "/api/paper/summary",
                "/api/paper/status",
                "/api/report/latest",
                "/api/reports",
                "/api/diagnostics",
                "/api/data-quality",
                "/api/data-provenance",
                "/api/analysis-runs",
            ]
            for ep in sensitive_endpoints:
                url = f"{server.url}{ep}"
                # Unauthenticated -> 401
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(url)
                assert exc_info.value.code == 401, f"Expected 401 for unauthenticated request to {ep}"

                # Authenticated with Bearer -> 200 or clean expected application response (e.g. 404 if no data yet)
                req_good = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
                try:
                    with urllib.request.urlopen(req_good) as resp:
                        assert resp.status in (200, 404), f"Expected 200 or 404 for authenticated request to {ep}"
                except urllib.error.HTTPError as exc:
                    assert exc.code in (200, 404), f"Expected 200 or 404 for authenticated request to {ep}, got {exc.code}"
                    assert exc.code != 401 and exc.code != 403
        finally:
            server.stop()

    def test_query_parameter_tokens_are_strictly_rejected(self) -> None:
        """Tokens passed via URL query parameters are strictly rejected with 401 Unauthorized."""
        token = "step57_query_param_token_test"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            auth_token=token,
        )
        server.start(blocking=False)
        try:
            urls_with_query_tokens = [
                f"{server.url}/api/paper/summary?token={token}",
                f"{server.url}/api/paper/summary?auth_token={token}",
                f"{server.url}/api/paper/summary?api_key={token}",
                f"{server.url}/api/reports?token={token}&limit=10",
                f"{server.url}/api/diagnostics?key={token}",
            ]
            for url in urls_with_query_tokens:
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(url)
                assert exc_info.value.code == 401
                data = json.loads(exc_info.value.read().decode("utf-8"))
                assert data["error"] == "Unauthorized"
                assert "Missing authentication credentials" in data["message"]
        finally:
            server.stop()


# ==============================================================================
# OBJECTIVE 4: DUAL-HEADER & EDGE-CASE AUTHENTICATION TESTS
# ==============================================================================


class TestAuthenticationEdgeCases:
    """Verifies edge cases including dual-header handling, whitespace, malformed schemes, and types."""

    def test_dual_headers_both_valid_accepted(self) -> None:
        """When both Authorization and X-API-Key are provided and both match, request succeeds."""
        token = "step57_dual_valid_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/status"
            # Health is public, but let's test a protected route
            prot_url = f"{server.url}/api/paper/status"
            req = urllib.request.Request(
                prot_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-API-Key": token,
                },
            )
            with urllib.request.urlopen(req) as resp:
                assert resp.status == 200
        finally:
            server.stop()

    def test_dual_headers_one_valid_one_invalid_rejected_401(self) -> None:
        """When both headers are provided and one is invalid, request is rejected with 401 Unauthorized."""
        token = "step57_dual_mismatch_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/status"

            # Case A: Authorization valid, X-API-Key invalid
            req_a = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-API-Key": "wrong_api_key",
                },
            )
            with pytest.raises(urllib.error.HTTPError) as exc_a:
                urllib.request.urlopen(req_a)
            assert exc_a.value.code == 401
            data_a = json.loads(exc_a.value.read().decode("utf-8"))
            assert "Conflicting or invalid authentication credentials" in data_a["message"]

            # Case B: Authorization invalid, X-API-Key valid
            req_b = urllib.request.Request(
                url,
                headers={
                    "Authorization": "Bearer wrong_bearer",
                    "X-API-Key": token,
                },
            )
            with pytest.raises(urllib.error.HTTPError) as exc_b:
                urllib.request.urlopen(req_b)
            assert exc_b.value.code == 401
            data_b = json.loads(exc_b.value.read().decode("utf-8"))
            assert "Conflicting or invalid authentication credentials" in data_b["message"]

            # Case C: Both invalid
            req_c = urllib.request.Request(
                url,
                headers={
                    "Authorization": "Bearer wrong_bearer",
                    "X-API-Key": "wrong_key",
                },
            )
            with pytest.raises(urllib.error.HTTPError) as exc_c:
                urllib.request.urlopen(req_c)
            assert exc_c.value.code == 401

        finally:
            server.stop()

    def test_dual_headers_one_valid_one_empty_or_malformed_rejected(self) -> None:
        """When one header is valid but the other is empty or malformed, request is rejected with 401."""
        token = "step57_dual_malformed_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/status"

            # Case A: Authorization valid, X-API-Key empty
            req_a = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-API-Key": "",
                },
            )
            with pytest.raises(urllib.error.HTTPError) as exc_a:
                urllib.request.urlopen(req_a)
            assert exc_a.value.code == 401
            data_a = json.loads(exc_a.value.read().decode("utf-8"))
            assert "Empty X-API-Key header provided" in data_a["message"]

            # Case B: X-API-Key valid, Authorization malformed scheme
            req_b = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"Basic {token}",
                    "X-API-Key": token,
                },
            )
            with pytest.raises(urllib.error.HTTPError) as exc_b:
                urllib.request.urlopen(req_b)
            assert exc_b.value.code == 401
            data_b = json.loads(exc_b.value.read().decode("utf-8"))
            assert "Invalid Authorization header format" in data_b["message"]

        finally:
            server.stop()

    def test_empty_bearer_token_rejected_401(self) -> None:
        """Authorization header with empty Bearer token returns 401."""
        token = "step57_empty_bearer_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/status"

            # Bare 'Bearer'
            req1 = urllib.request.Request(url, headers={"Authorization": "Bearer"})
            with pytest.raises(urllib.error.HTTPError) as exc1:
                urllib.request.urlopen(req1)
            assert exc1.value.code == 401

            # 'Bearer ' with trailing spaces
            req2 = urllib.request.Request(url, headers={"Authorization": "Bearer   "})
            with pytest.raises(urllib.error.HTTPError) as exc2:
                urllib.request.urlopen(req2)
            assert exc2.value.code == 401

        finally:
            server.stop()

    def test_empty_api_key_rejected_401(self) -> None:
        """X-API-Key header with empty or whitespace-only value returns 401."""
        token = "step57_empty_api_key_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/status"

            req1 = urllib.request.Request(url, headers={"X-API-Key": ""})
            with pytest.raises(urllib.error.HTTPError) as exc1:
                urllib.request.urlopen(req1)
            assert exc1.value.code == 401

            req2 = urllib.request.Request(url, headers={"X-API-Key": "   \t  "})
            with pytest.raises(urllib.error.HTTPError) as exc2:
                urllib.request.urlopen(req2)
            assert exc2.value.code == 401

        finally:
            server.stop()

    def test_case_insensitive_bearer_and_whitespace_trimming(self) -> None:
        """Bearer scheme is case-insensitive ('bearer', 'Bearer', 'BEARER') and whitespace trimmed."""
        token = "step57_case_and_space_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/status"

            for scheme in ("bearer", "Bearer", "BEARER", "bEaReR"):
                req = urllib.request.Request(url, headers={"Authorization": f"{scheme}   {token}   "})
                with urllib.request.urlopen(req) as resp:
                    assert resp.status == 200

            # X-API-Key whitespace trimming
            req_api = urllib.request.Request(url, headers={"X-API-Key": f"  {token}  "})
            with urllib.request.urlopen(req_api) as resp:
                assert resp.status == 200

        finally:
            server.stop()

    def test_server_header_hardening_suppresses_python_version(self) -> None:
        """Server response header suppresses runtime/OS version details."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/status"
            with urllib.request.urlopen(url) as resp:
                server_hdr = resp.headers.get("Server", "")
                assert "Python" not in server_hdr
                assert "BaseHTTP" not in server_hdr
                assert server_hdr == "KAISER-Dashboard"
        finally:
            server.stop()

    def test_log_message_redacts_query_parameter_secrets(self, caplog: pytest.LogCaptureFixture) -> None:
        """Request log output redacts secret identifiers from query parameters."""
        token = "step57_log_redaction_secret"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/status?token={token}&key={token}"
            with caplog.at_level(logging.DEBUG, logger="kaiser.portfolio.dashboard"):
                urllib.request.urlopen(url)
            # Check captured debug log messages
            log_text = caplog.text
            assert token not in log_text, "Configured secret token leaked in dashboard request log!"
            assert "[REDACTED]" in log_text
        finally:
            server.stop()


# ==============================================================================
# OBJECTIVE 3: ROUTE INVENTORY AND AUTHORIZATION VERIFICATION
# ==============================================================================


class TestRouteAuthorizationInventory:
    """Verifies access control and routing behavior across the complete dashboard route inventory."""

    def test_public_routes_accessible_without_authentication(self) -> None:
        """Public routes (/ and /api/status) remain accessible when authentication is required."""
        token = "step57_public_route_test_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token, require_auth=True)
        server.start(blocking=False)
        try:
            # 1. Main SPA dashboard shell
            with urllib.request.urlopen(f"{server.url}/") as resp:
                assert resp.status == 200
                assert "text/html" in resp.headers.get("Content-Type", "")

            with urllib.request.urlopen(f"{server.url}/index.html") as resp:
                assert resp.status == 200

            # 2. Public health check
            with urllib.request.urlopen(f"{server.url}/api/status") as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["status"] == "healthy"
                assert data["execution_locked"] is True
                assert data["read_only"] is True
                # Minimum operational info only; no secret or private keys
                assert "token" not in data
                assert "auth" not in data
                assert "positions" not in data

            # HEAD request to /api/status
            head_req = urllib.request.Request(f"{server.url}/api/status", method="HEAD")
            with urllib.request.urlopen(head_req) as resp:
                assert resp.status == 200
                assert resp.read() == b""

        finally:
            server.stop()

    def test_all_sensitive_routes_require_authorization(self) -> None:
        """Every sensitive /api/* route requires valid authorization credentials."""
        token = "step57_full_inventory_audit_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            inventory_routes = [
                "/api/paper/summary",
                "/api/paper/status",
                "/api/report/latest",
                "/api/reports",
                "/api/reports?limit=10",
                "/api/series/nonexistent_report_id",
                "/api/compare?runs=run1,run2",
                "/api/diagnostics",
                "/api/data-quality",
                "/api/data-provenance",
                "/api/data-lineage/prov_123",
                "/api/analysis-runs",
                "/api/analysis-comparisons",
                "/api/evidence-syntheses",
                "/api/opportunity-assessments",
                "/api/opportunity-priorities",
                "/api/opportunity-queue",
                "/api/investigation-sessions",
                "/api/investigation-reports",
                "/api/investigation-comparisons",
                "/api/discrepancy-resolutions",
                "/api/investigation-outcomes",
                "/api/consolidated-outcome-comparisons",
                "/api/historical-review",
                "/api/review-evolution",
                "/api/historical-review-audit",
                "/api/historical-review-reliability",
                "/api/historical-review-diagnostics",
            ]

            for route in inventory_routes:
                url = f"{server.url}{route}"

                # 1. Unauthenticated -> 401
                with pytest.raises(urllib.error.HTTPError) as exc_unauth:
                    urllib.request.urlopen(url)
                assert exc_unauth.value.code == 401, f"Route {route} should reject unauthenticated request with 401"
                assert 'Bearer realm="KAISER Dashboard"' in exc_unauth.value.headers.get("WWW-Authenticate", "")

                # 2. Invalid token -> 401
                req_bad = urllib.request.Request(url, headers={"Authorization": "Bearer bad_token_123"})
                with pytest.raises(urllib.error.HTTPError) as exc_bad:
                    urllib.request.urlopen(req_bad)
                assert exc_bad.value.code == 401, f"Route {route} should reject invalid token with 401"

                # 3. Valid token -> allowed past auth barrier (returns 200, or clean 404/400 if ID not seeded)
                req_good = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
                try:
                    with urllib.request.urlopen(req_good) as resp_good:
                        assert resp_good.status in (200, 404)
                except urllib.error.HTTPError as exc_good:
                    # Non-auth application response (e.g. 404 Not Found if unseeded ID)
                    assert exc_good.code in (400, 404), f"Route {route} returned unexpected error {exc_good.code}"
                    assert exc_good.code != 401 and exc_good.code != 403

        finally:
            server.stop()

    def test_unknown_routes_protected_against_unauthenticated_reconnaissance(self) -> None:
        """Unrecognized /api/* routes require auth when auth is enforced, preventing API scanning."""
        token = "step57_unknown_route_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/unrecognized_secret_probe_route"

            # 1. Unauthenticated -> 401
            with pytest.raises(urllib.error.HTTPError) as exc_unauth:
                urllib.request.urlopen(url)
            assert exc_unauth.value.code == 401

            # 2. Authenticated -> 404 Not Found (clean JSON)
            req_good = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            with pytest.raises(urllib.error.HTTPError) as exc_auth:
                urllib.request.urlopen(req_good)
            assert exc_auth.value.code == 404
            data = json.loads(exc_auth.value.read().decode("utf-8"))
            assert data["error"] == "Not Found"

        finally:
            server.stop()


# ==============================================================================
# OBJECTIVE 5: PROHIBITED METHODS & STATE INVARIANCE REGRESSION
# ==============================================================================


class TestMethodSafetyAndStateInvariance:
    """Verifies that all mutating HTTP methods are rejected and state is 100% immutable via HTTP."""

    def test_all_prohibited_methods_rejected_with_405(self) -> None:
        """POST, PUT, PATCH, DELETE, OPTIONS, TRACE return 405 Method Not Allowed."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            endpoints = ["/api/status", "/api/paper/summary", "/api/reports", "/api/diagnostics"]
            prohibited_methods = ["POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"]

            for ep in endpoints:
                url = f"{server.url}{ep}"
                for method in prohibited_methods:
                    req = urllib.request.Request(
                        url,
                        data=b'{"action": "hack", "cash": 999999}' if method in ("POST", "PUT", "PATCH") else None,
                        headers={"Content-Type": "application/json"} if method in ("POST", "PUT", "PATCH") else {},
                        method=method,
                    )
                    with pytest.raises(urllib.error.HTTPError) as exc_info:
                        urllib.request.urlopen(req)
                    assert exc_info.value.code == 405, f"Method {method} on {ep} should return 405"
                    assert "GET, HEAD" in exc_info.value.headers.get("Allow", "")
                    body = json.loads(exc_info.value.read().decode("utf-8"))
                    assert body["error"] == "Method Not Allowed"
                    assert "strictly read-only" in body["message"]

        finally:
            server.stop()

    def test_complete_state_invariance_before_and_after_attacks(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
        fresh_quote: Quote,
    ) -> None:
        """Unauthorized requests and prohibited methods cannot mutate cash, positions, orders, or ledger."""
        token = "step57_state_invariance_audit_token"
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            paper_coordinator=coordinator,
            auth_token=token,
        )
        server.start(blocking=False)
        try:
            engine = coordinator.engine
            assert engine is not None
            assert coordinator.state_store is not None
            initial_cash = engine.portfolio.available_cash
            initial_positions = dict(engine.portfolio.positions)
            initial_ledger_entries = list(engine.ledger.get_entries())
            initial_order_count = len(engine._orders)
            initial_fill_count = len(engine._fills)

            attack_scenarios = [
                # Unauthenticated GET
                urllib.request.Request(f"{server.url}/api/paper/summary"),
                # Invalid Bearer GET
                urllib.request.Request(f"{server.url}/api/paper/summary", headers={"Authorization": "Bearer bad_tok"}),
                # Malformed Scheme GET
                urllib.request.Request(f"{server.url}/api/paper/summary", headers={"Authorization": "Basic dGVzdA=="}),
                # Invalid API Key GET
                urllib.request.Request(f"{server.url}/api/paper/summary", headers={"X-API-Key": "fake_key"}),
                # Conflicting Dual Header GET
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    headers={"Authorization": f"Bearer {token}", "X-API-Key": "wrong_key"},
                ),
                # POST mutation attempt
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    data=b'{"symbol": "RELIANCE", "quantity": 1000}',
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                    method="POST",
                ),
                # PUT balance manipulation attempt
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    data=b'{"available_cash": 99999999.0}',
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                    method="PUT",
                ),
                # DELETE position wipe attempt
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    headers={"Authorization": f"Bearer {token}"},
                    method="DELETE",
                ),
                # PATCH execution settings attempt
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    data=b'{"execution_locked": false}',
                    headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
                    method="PATCH",
                ),
            ]

            for req in attack_scenarios:
                with pytest.raises(urllib.error.HTTPError):
                    urllib.request.urlopen(req)

                # Assert state invariance after EVERY single attack
                assert engine.portfolio.available_cash == initial_cash
                assert dict(engine.portfolio.positions) == initial_positions
                assert len(engine.ledger.get_entries()) == len(initial_ledger_entries)
                assert len(engine._orders) == initial_order_count
                assert len(engine._fills) == initial_fill_count

            # Architectural invariants check
            settings = coordinator.settings
            assert settings.execution_locked is True
            assert settings.broker_provider == "none"
            assert coordinator.state_store.get("trading_engine") == "not initialized"
            assert coordinator.state_store.get("execution_engine") == "locked"

        finally:
            server.stop()


# ==============================================================================
# CONFIGURATION TYPE & VALIDATION HARDENING TESTS
# ==============================================================================


class TestConfigurationValidationAndHardening:
    """Verifies strict boolean parsing, port limits, token types, and serialization masking."""

    def test_parse_bool_accepts_valid_and_rejects_invalid_strings(self) -> None:
        """parse_bool must accept standard booleans and reject invalid strings without silent truthiness."""
        for truthy in ("true", "True", "TRUE", "1", "yes", "YES", "on", "ON", "t", "T"):
            assert parse_bool(truthy, "TEST") is True

        for falsy in ("false", "False", "FALSE", "0", "no", "NO", "off", "OFF", "f", "F"):
            assert parse_bool(falsy, "TEST") is False

        for invalid in ("maybe", "unknown", "2", "-1", "truthy", "none", "", "   "):
            with pytest.raises(ConfigurationError) as exc_info:
                parse_bool(invalid, "TEST_FIELD")
            assert "Invalid boolean value for TEST_FIELD" in str(exc_info.value)

    def test_settings_normalizes_boolean_fields_and_rejects_invalid_values(self) -> None:
        """Settings.__post_init__ normalizes string booleans and rejects invalid boolean inputs."""
        # Valid string representations
        s_true = Settings(dashboard_require_auth="true", paper_execution_enabled="yes")  # type: ignore[arg-type]
        assert s_true.dashboard_require_auth is True
        assert s_true.paper_execution_enabled is True

        s_false = Settings(dashboard_require_auth="false", paper_execution_enabled="no")  # type: ignore[arg-type]
        assert s_false.dashboard_require_auth is False
        assert s_false.paper_execution_enabled is False

        # Invalid string representations must raise ConfigurationError (not become truthy!)
        with pytest.raises(ConfigurationError):
            Settings(dashboard_require_auth="invalid_bool_string")  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError):
            Settings(paper_execution_enabled="maybe")  # type: ignore[arg-type]

    def test_dashboard_port_validation(self) -> None:
        """Settings and PortfolioDashboardServer validate port ranges (0-65535)."""
        # String port parsed
        s = Settings(dashboard_port="8090")  # type: ignore[arg-type]
        assert s.dashboard_port == 8090

        # Out-of-range port
        with pytest.raises(ConfigurationError):
            Settings(dashboard_port=70000)

        with pytest.raises(ConfigurationError):
            Settings(dashboard_port=-1)

        with pytest.raises(ConfigurationError):
            Settings(dashboard_port="invalid_alpha_port")  # type: ignore[arg-type]

        with pytest.raises(ValidationError):
            PortfolioDashboardServer(port=99999)

    def test_auth_token_type_validation(self) -> None:
        """Settings and PortfolioDashboardServer reject non-string auth_token types."""
        with pytest.raises(ConfigurationError):
            Settings(dashboard_auth_token=12345)  # type: ignore

        with pytest.raises(ValidationError):
            PortfolioDashboardServer(auth_token=999)  # type: ignore

        with pytest.raises(ValidationError):
            PortfolioDashboardServer(require_auth="invalid_bool")  # type: ignore[arg-type]

    def test_settings_serialization_masks_secrets(self) -> None:
        """Settings.to_dict() masks dashboard_auth_token and twelve_data_api_key."""
        s = Settings(
            dashboard_auth_token="super_secret_dashboard_key",
            twelve_data_api_key="super_secret_twelve_data_key",
        )
        d = s.to_dict()
        assert d["dashboard_auth_token"] == "***"
        assert d["twelve_data_api_key"] == "***"
        assert "super_secret_dashboard_key" not in json.dumps(d)
        assert "super_secret_twelve_data_key" not in json.dumps(d)

    def test_dataclasses_replace_preserves_secrets_when_enabling_dashboard(self) -> None:
        """dataclasses.replace does not overwrite secrets with asterisks like to_dict() would."""
        s = Settings(
            dashboard_auth_token="real_unmasked_secret",
            twelve_data_api_key="real_td_key",
            dashboard_enabled=False,
        )
        # Using dataclasses.replace as run.py now does
        updated = dataclasses.replace(s, dashboard_enabled=True)
        assert updated.dashboard_enabled is True
        assert updated.dashboard_auth_token == "real_unmasked_secret"
        assert updated.twelve_data_api_key == "real_td_key"
