"""KAISER — Phase 2, Step 58: Security Regression, Configuration Matrix & Operational Readiness Verification.

Comprehensive verification of:
1. OBJECTIVE 2: Configuration Matrix Tests:
   - Dashboard disabled, with no authentication token.
   - Dashboard disabled, with authentication configured.
   - Dashboard enabled, authentication not required, no token.
   - Dashboard enabled, authentication not required, token configured.
   - Dashboard enabled, authentication required, no token (fails closed 403).
   - Dashboard enabled, authentication required, valid token.
   - Dashboard enabled, authentication required, invalid or empty token.
   - Invalid boolean configuration values.
   - Missing and malformed environment variables.
   - Invalid dashboard host values.
   - Valid and invalid port values (ports 0, 1, 65535, 65536, and production port 0 policy).

2. OBJECTIVE 3: Complete Dashboard Route Authorization Matrix:
   - Public vs protected route classification across all 28+ routes.
   - Safe responses for missing, invalid, and valid credentials.
   - Unknown routes requiring auth when auth enforced; clean 404 when authenticated.
   - Malformed path handling (traversal, null bytes, long URIs).
   - Read-only guarantees: zero HTTP mutation routes.

3. OBJECTIVE 4: Secret and Error-Handling Regressions:
   - Settings.to_dict() masking.
   - Startup banner zero secrets.
   - Zero reflection in auth failures, query parameters, logs, and unexpected exceptions.
   - Redaction preserves runtime credentials on frozen Settings.

4. OBJECTIVE 5: HTTP Method and State-Invariance Tests:
   - Prohibited methods (POST, PUT, PATCH, DELETE, OPTIONS, TRACE) rejected with 405.
   - State invariance before and after attacks (cash, positions, orders, ledger, locks).

5. OBJECTIVE 6: Startup and Runtime Readiness:
   - Bootstrap dashboard disabled vs enabled.
   - Safe server shutdown without socket or thread leaks.
   - Fail-closed behavior on missing token.

6. OBJECTIVE 7: Trading Isolation Invariants:
   - execution_locked=True, broker_provider="none", trading_engine="not initialized", paper_execution_enabled=False.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from pathlib import Path
import time
from typing import Any, Dict, Generator, List, Optional
import urllib.error
import urllib.request
import pytest

from app.bootstrap import bootstrap_system, format_banner
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
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


# ==============================================================================
# FIXTURES
# ==============================================================================


@pytest.fixture
def event_bus() -> EventBus:
    return EventBus(name="test_step58_event_bus")


@pytest.fixture
def state_store() -> StateStore:
    store = StateStore(name="test_step58_state_store")
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
    db_file = str(tmp_path / "paper_step58_ledger.db")
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
# OBJECTIVE 2: CONFIGURATION MATRIX TESTS
# ==============================================================================


class TestConfigurationMatrix:
    """Verifies all mandatory combinations of dashboard and security settings."""

    def test_combination_01_dashboard_disabled_no_token(self) -> None:
        """Dashboard disabled, no auth token: default safe state, no listener started."""
        s = Settings(dashboard_enabled=False, dashboard_auth_token=None)
        assert s.dashboard_enabled is False
        assert s.dashboard_auth_token is None
        assert s.dashboard_require_auth is False
        assert s.execution_locked is True
        assert s.broker_provider == "none"

        # Verify bootstrap does not start listener
        status = bootstrap_system(settings=s, log_to_file=False)
        assert status.portfolio_dashboard_server is not None
        assert status.portfolio_dashboard_server.is_running is False

    def test_combination_02_dashboard_disabled_with_token(self) -> None:
        """Dashboard disabled, token configured: token stored safely, no listener started."""
        token = "step58_disabled_token"
        s = Settings(dashboard_enabled=False, dashboard_auth_token=token)
        assert s.dashboard_enabled is False
        assert s.dashboard_auth_token == token

        # Verify token is masked in serialization
        d = s.to_dict()
        assert d["dashboard_auth_token"] == "***"
        assert token not in json.dumps(d)

        status = bootstrap_system(settings=s, log_to_file=False)
        assert status.portfolio_dashboard_server is not None
        assert status.portfolio_dashboard_server.is_running is False

    def test_combination_03_dashboard_enabled_auth_not_required_no_token(self) -> None:
        """Dashboard enabled, require_auth=False, no token: local unauthenticated dev mode."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, require_auth=False, auth_token=None)
        server.start(blocking=False)
        try:
            assert server.is_running is True
            # Public health check
            with urllib.request.urlopen(f"{server.url}/api/status") as resp:
                assert resp.status == 200
                data = json.loads(resp.read().decode("utf-8"))
                assert data["status"] == "healthy"
                assert data["read_only"] is True

            # In local dev mode, protected route is accessible without credentials
            with urllib.request.urlopen(f"{server.url}/api/paper/summary") as resp:
                assert resp.status == 200
        finally:
            server.stop()

    def test_combination_04_dashboard_enabled_auth_not_required_token_configured(self) -> None:
        """Dashboard enabled, require_auth=False, token configured: token presence enforces auth."""
        token = "step58_token_enforces_auth"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, require_auth=False, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"
            # Unauthenticated -> rejected with 401
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(url)
            assert exc_info.value.code == 401

            # Authenticated -> 200
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(req) as resp:
                assert resp.status == 200
        finally:
            server.stop()

    def test_combination_05_dashboard_enabled_auth_required_no_token_fails_closed(self) -> None:
        """Dashboard enabled, require_auth=True, no token: fails closed with HTTP 403 Forbidden."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, require_auth=True, auth_token=None)
        server.start(blocking=False)
        try:
            # Public health check remains operational
            with urllib.request.urlopen(f"{server.url}/api/status") as resp:
                assert resp.status == 200

            # Protected endpoint fails closed
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(f"{server.url}/api/paper/summary")
            assert exc_info.value.code == 403
            data = json.loads(exc_info.value.read().decode("utf-8"))
            assert data["error"] == "Forbidden"
            assert "no server authentication token is configured" in data["message"]
        finally:
            server.stop()

    def test_combination_06_dashboard_enabled_auth_required_valid_token(self) -> None:
        """Dashboard enabled, require_auth=True, valid token: normal authenticated operation."""
        token = "step58_valid_auth_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, require_auth=True, auth_token=token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary"
            # Unauthenticated -> 401
            with pytest.raises(urllib.error.HTTPError) as exc_unauth:
                urllib.request.urlopen(url)
            assert exc_unauth.value.code == 401

            # Bearer token -> 200
            req_bearer = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            with urllib.request.urlopen(req_bearer) as resp:
                assert resp.status == 200

            # X-API-Key -> 200
            req_key = urllib.request.Request(url, headers={"X-API-Key": token})
            with urllib.request.urlopen(req_key) as resp:
                assert resp.status == 200
        finally:
            server.stop()

    def test_combination_07_dashboard_enabled_auth_required_whitespace_token_fails_closed(self) -> None:
        """Dashboard enabled, require_auth=True, whitespace token: normalizes to None and fails closed."""
        server = PortfolioDashboardServer(
            host="127.0.0.1",
            port=0,
            require_auth=True,
            auth_token="   \t \n  ",
        )
        assert server.auth_token is None
        server.start(blocking=False)
        try:
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(f"{server.url}/api/paper/summary")
            assert exc_info.value.code == 403
        finally:
            server.stop()

    def test_combination_08_invalid_boolean_configuration_values_rejected(self) -> None:
        """Ambiguous/invalid boolean strings must raise ConfigurationError rather than coerce to True."""
        invalid_booleans = ("maybe", "unknown", "2", "-1", "none", "truthy", "sure", "", "   ")
        for inv in invalid_booleans:
            with pytest.raises(ConfigurationError):
                parse_bool(inv, "TEST_FIELD")

            with pytest.raises(ConfigurationError):
                Settings(dashboard_require_auth=inv)  # type: ignore[arg-type]

            with pytest.raises(ConfigurationError):
                Settings(dashboard_enabled=inv)  # type: ignore[arg-type]

    def test_combination_09_missing_and_malformed_environment_variables(self) -> None:
        """get_settings() handles missing keys with safe defaults and rejects malformed values."""
        # Missing keys default safely
        s_default = get_settings(env_source={})
        assert s_default.dashboard_enabled is False
        assert s_default.dashboard_require_auth is False
        assert s_default.dashboard_auth_token is None
        assert s_default.dashboard_port == 8080
        assert s_default.dashboard_host == "127.0.0.1"
        assert s_default.execution_locked is True
        assert s_default.broker_provider == "none"

        # Malformed port in environment raises ConfigurationError
        with pytest.raises(ConfigurationError) as exc_port:
            get_settings(env_source={"DASHBOARD_PORT": "invalid_alpha_port"})
        assert "Invalid DASHBOARD_PORT" in str(exc_port.value)

        # Malformed boolean in environment raises ConfigurationError
        with pytest.raises(ConfigurationError) as exc_bool:
            get_settings(env_source={"DASHBOARD_REQUIRE_AUTH": "not_a_bool"})
        assert "Invalid boolean value" in str(exc_bool.value)

        # Malformed numeric rate limit in environment raises ConfigurationError
        with pytest.raises(ConfigurationError) as exc_num:
            get_settings(env_source={"TWELVE_DATA_RATE_LIMIT_PER_MINUTE": "not_an_int"})
        assert "Invalid numeric configuration" in str(exc_num.value)

    def test_combination_10_invalid_dashboard_host_values_rejected(self) -> None:
        """Non-loopback host addresses are strictly rejected with Security constraint violation."""
        prohibited_hosts = (
            "0.0.0.0",
            "192.168.1.100",
            "10.0.0.1",
            "172.16.0.1",
            "example.com",
            "8.8.8.8",
            "255.255.255.255",
        )
        for bad_host in prohibited_hosts:
            with pytest.raises(ConfigurationError):
                Settings(dashboard_host=bad_host)

            with pytest.raises(ValidationError) as exc_val:
                PortfolioDashboardServer(host=bad_host, port=0)
            assert "Security constraint violation" in str(exc_val.value)

    def test_combination_11_port_validation_0_1_65535_65536(self) -> None:
        """Inspects and verifies port validation across ports 0, 1, 65535, 65536."""
        # Port 1 (valid low edge)
        s_port1 = Settings(dashboard_port=1)
        assert s_port1.dashboard_port == 1
        srv_port1 = PortfolioDashboardServer(port=1)
        assert srv_port1.port == 1

        # Port 65535 (valid high edge)
        s_port65535 = Settings(dashboard_port=65535)
        assert s_port65535.dashboard_port == 65535
        srv_port65535 = PortfolioDashboardServer(port=65535)
        assert srv_port65535.port == 65535

        # Port 0 in development/test (valid ephemeral binding)
        s_port0_dev = Settings(dashboard_port=0, app_env="development")
        assert s_port0_dev.dashboard_port == 0
        srv_port0 = PortfolioDashboardServer(port=0)
        assert srv_port0.port == 0

        # Port 0 in production is strictly prohibited to prevent ambiguous random listening ports
        with pytest.raises(ConfigurationError) as exc_p0_prod:
            Settings(dashboard_port=0, app_env="production")
        assert "Ephemeral port binding (port 0) is reserved for test/development" in str(exc_p0_prod.value)

        # Port 65536 (out-of-range high)
        with pytest.raises(ConfigurationError):
            Settings(dashboard_port=65536)
        with pytest.raises(ValidationError):
            PortfolioDashboardServer(port=65536)

        # Port -1 (out-of-range low)
        with pytest.raises(ConfigurationError):
            Settings(dashboard_port=-1)
        with pytest.raises(ValidationError):
            PortfolioDashboardServer(port=-1)


# ==============================================================================
# OBJECTIVE 3: DASHBOARD ROUTE AUTHORIZATION MATRIX
# ==============================================================================


class TestDashboardRouteAuthorizationMatrix:
    """Verifies access control, methods, and response behavior across all routes."""

    @pytest.fixture
    def authenticated_server(self) -> Generator[PortfolioDashboardServer, None, None]:
        token = "step58_matrix_test_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=token)
        server.start(blocking=False)
        try:
            yield server
        finally:
            server.stop()

    def test_public_routes(self, authenticated_server: PortfolioDashboardServer) -> None:
        """Public routes (/ and /api/status) are accessible without credentials."""
        # 1. SPA Shell (/)
        with urllib.request.urlopen(f"{authenticated_server.url}/") as resp:
            assert resp.status == 200
            assert "text/html" in resp.headers.get("Content-Type", "")

        # 2. SPA Shell (/index.html)
        with urllib.request.urlopen(f"{authenticated_server.url}/index.html") as resp:
            assert resp.status == 200
            assert "text/html" in resp.headers.get("Content-Type", "")

        # 3. Operational health check (/api/status)
        with urllib.request.urlopen(f"{authenticated_server.url}/api/status") as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
            assert data["status"] == "healthy"
            assert data["execution_locked"] is True
            assert data["read_only"] is True
            # Operational status only: zero secrets or sensitive holdings
            assert "token" not in data
            assert "positions" not in data
            assert "api_key" not in data

    def test_protected_route_inventory(self, authenticated_server: PortfolioDashboardServer) -> None:
        """All protected routes reject unauthenticated requests (401) and allow authenticated requests."""
        token = "step58_matrix_test_token"
        protected_routes = [
            "/api/paper/summary",
            "/api/paper/status",
            "/api/report/latest",
            "/api/reports",
            "/api/series/nonexistent_id",
            "/api/compare?runs=runA,runB",
            "/api/diagnostics",
            "/api/data-quality",
            "/api/data-provenance",
            "/api/data-lineage/prov_abc",
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

        for route in protected_routes:
            url = f"{authenticated_server.url}{route}"

            # 1. Unauthenticated request must return 401
            with pytest.raises(urllib.error.HTTPError) as exc_unauth:
                urllib.request.urlopen(url)
            assert exc_unauth.value.code == 401, f"Route {route} did not return 401 unauthenticated"
            assert 'Bearer realm="KAISER Dashboard"' in exc_unauth.value.headers.get("WWW-Authenticate", "")

            # 2. Invalid credentials must return 401
            req_bad = urllib.request.Request(url, headers={"Authorization": "Bearer bad_secret"})
            with pytest.raises(urllib.error.HTTPError) as exc_bad:
                urllib.request.urlopen(req_bad)
            assert exc_bad.value.code == 401

            # 3. Valid credentials allowed past auth barrier (returns 200 or clean 404/400 for unseeded test IDs)
            req_good = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            try:
                with urllib.request.urlopen(req_good) as resp_good:
                    assert resp_good.status in (200, 404)
            except urllib.error.HTTPError as exc_good:
                assert exc_good.code in (400, 404)
                assert exc_good.code not in (401, 403)

    def test_unknown_routes_protected_and_clean_404(self, authenticated_server: PortfolioDashboardServer) -> None:
        """Unrecognized /api/* paths require auth (401) and return clean 404 when authenticated."""
        token = "step58_matrix_test_token"
        unknown_url = f"{authenticated_server.url}/api/unrecognized_secret_probe_route"

        # 1. Unauthenticated unknown route -> 401
        with pytest.raises(urllib.error.HTTPError) as exc_unauth:
            urllib.request.urlopen(unknown_url)
        assert exc_unauth.value.code == 401

        # 2. Authenticated unknown route -> clean JSON 404
        req_auth = urllib.request.Request(unknown_url, headers={"Authorization": f"Bearer {token}"})
        with pytest.raises(urllib.error.HTTPError) as exc_auth:
            urllib.request.urlopen(req_auth)
        assert exc_auth.value.code == 404
        data = json.loads(exc_auth.value.read().decode("utf-8"))
        assert data["error"] == "Not Found"

    def test_malformed_paths_rejected(self, authenticated_server: PortfolioDashboardServer) -> None:
        """Path traversal, script injection, and oversized URIs are rejected safely."""
        token = "step58_matrix_test_token"

        # 1. Prohibited traversal sequences return 400 Bad Request immediately (even unauthenticated)
        traversal_paths = [
            "/api/report/..\\..\\windows\\system32",
            "/api/report/something/..",
        ]
        for tp in traversal_paths:
            with pytest.raises(urllib.error.HTTPError) as exc_trav:
                urllib.request.urlopen(f"{authenticated_server.url}{tp}")
            assert exc_trav.value.code == 400
            data = json.loads(exc_trav.value.read().decode("utf-8"))
            assert data["error"] == "Bad Request"

        # 2. Encoded traversal or script injection in resource ID returns 400 Bad Request when authenticated
        bad_id_paths = [
            "/api/report/..%2f..%2fetc/passwd",
            "/api/report/<script>alert(1)</script>",
        ]
        for bip in bad_id_paths:
            req = urllib.request.Request(f"{authenticated_server.url}{bip}", headers={"Authorization": f"Bearer {token}"})
            with pytest.raises(urllib.error.HTTPError) as exc_bip:
                urllib.request.urlopen(req)
            assert exc_bip.value.code == 400
            data = json.loads(exc_bip.value.read().decode("utf-8"))
            assert data["error"] == "Bad Request"

        # 3. URI exceeding MAX_URI_LENGTH (2048 chars) returns 414 URI Too Long
        oversized_url = f"{authenticated_server.url}/api/status?padding={'A' * 2100}"
        with pytest.raises(urllib.error.HTTPError) as exc_len:
            urllib.request.urlopen(oversized_url)
        assert exc_len.value.code == 414


# ==============================================================================
# OBJECTIVE 4: SECRET AND ERROR-HANDLING REGRESSION TESTS
# ==============================================================================


class TestSecretAndErrorHandlingRegressions:
    """Verifies that secrets are never leaked in serialization, logs, errors, or banners."""

    def test_settings_to_dict_redaction_preserves_runtime_secrets(self) -> None:
        """Settings.to_dict() masks secrets without mutating the running frozen Settings object."""
        real_dashboard_token = "step58_real_dashboard_secret_key"
        real_twelve_data_key = "step58_real_td_api_key_12345"

        s = Settings(
            dashboard_auth_token=real_dashboard_token,
            twelve_data_api_key=real_twelve_data_key,
        )

        d = s.to_dict()
        assert d["dashboard_auth_token"] == "***"
        assert d["twelve_data_api_key"] == "***"
        assert real_dashboard_token not in json.dumps(d)
        assert real_twelve_data_key not in json.dumps(d)

        # Runtime frozen object retains original values untouched
        assert s.dashboard_auth_token == real_dashboard_token
        assert s.twelve_data_api_key == real_twelve_data_key

    def test_format_banner_contains_zero_secrets(self) -> None:
        """System startup banner does not leak secrets, tokens, or private configurations."""
        secret_token = "step58_banner_secret"
        s = Settings(dashboard_auth_token=secret_token)
        status = bootstrap_system(settings=s, log_to_file=False)
        banner = format_banner(status)
        assert secret_token not in banner
        assert "token" not in banner.lower()
        assert "Execution engine: locked" in banner

    def test_query_parameter_credentials_rejected_and_redacted_in_logs(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Query parameters containing credentials are rejected with 401 and redacted in logs."""
        secret_token = "step58_query_redaction_token"
        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=secret_token)
        server.start(blocking=False)
        try:
            url = f"{server.url}/api/paper/summary?token={secret_token}&api_key={secret_token}&password={secret_token}"
            with caplog.at_level(logging.DEBUG, logger="kaiser.portfolio.dashboard"):
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(url)
                assert exc_info.value.code == 401

            # Assert log sanitization
            log_output = caplog.text
            assert secret_token not in log_output, "Secret token leaked in dashboard request logs!"
            assert "[REDACTED]" in log_output
        finally:
            server.stop()

    def test_auth_error_responses_never_echo_credentials(self) -> None:
        """401 and 403 responses never reflect supplied or expected credentials."""
        secret_server_token = "step58_secret_server_token"
        attacker_supplied_token = "step58_attacker_probe_token"

        server = PortfolioDashboardServer(host="127.0.0.1", port=0, auth_token=secret_server_token)
        server.start(blocking=False)
        try:
            req = urllib.request.Request(
                f"{server.url}/api/paper/summary",
                headers={"Authorization": f"Bearer {attacker_supplied_token}"},
            )
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req)
            assert exc_info.value.code == 401
            body = exc_info.value.read().decode("utf-8")
            assert secret_server_token not in body
            assert attacker_supplied_token not in body
        finally:
            server.stop()

    def test_validation_errors_never_disclose_secrets(self) -> None:
        """Configuration and validation error messages do not disclose secret values."""
        with pytest.raises(ConfigurationError) as exc_info:
            Settings(dashboard_auth_token=99999)  # type: ignore[arg-type]
        err_msg = str(exc_info.value)
        assert "99999" not in err_msg
        assert "expected string or None" in err_msg


# ==============================================================================
# OBJECTIVE 5: HTTP METHOD SAFETY & STATE INVARIANCE
# ==============================================================================


class TestMethodSafetyAndStateInvariance:
    """Verifies that unsupported HTTP methods are rejected and cannot mutate state."""

    def test_all_mutating_methods_rejected_405(self) -> None:
        """POST, PUT, PATCH, DELETE, OPTIONS, TRACE return 405 Method Not Allowed."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        try:
            test_routes = ["/api/status", "/api/paper/summary", "/api/reports"]
            prohibited_methods = ["POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"]

            for route in test_routes:
                url = f"{server.url}{route}"
                for method in prohibited_methods:
                    req = urllib.request.Request(
                        url,
                        data=b'{"action": "manipulate"}' if method in ("POST", "PUT", "PATCH") else None,
                        headers={"Content-Type": "application/json"} if method in ("POST", "PUT", "PATCH") else {},
                        method=method,
                    )
                    with pytest.raises(urllib.error.HTTPError) as exc_info:
                        urllib.request.urlopen(req)
                    assert exc_info.value.code == 405, f"Method {method} on {route} should return 405"
                    assert "GET, HEAD" in exc_info.value.headers.get("Allow", "")
        finally:
            server.stop()

    def test_state_invariance_under_authorized_and_unauthorized_prohibited_methods(
        self,
        coordinator: PaperTradingWorkflowCoordinator,
    ) -> None:
        """Simulated attacks (both with and without valid token) cannot mutate portfolio state."""
        token = "step58_state_invariance_token"
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
            initial_cash = engine.portfolio.available_cash
            initial_positions = dict(engine.portfolio.positions)
            initial_order_count = len(engine._orders)
            initial_fill_count = len(engine._fills)
            initial_ledger_count = len(engine.ledger.get_entries())

            # Scenario A: Unauthenticated mutating attempts
            unauth_attacks = [
                urllib.request.Request(f"{server.url}/api/paper/summary", data=b'{"cash": 1e6}', method="POST"),
                urllib.request.Request(f"{server.url}/api/paper/summary", data=b'{"cash": 1e6}', method="PUT"),
                urllib.request.Request(f"{server.url}/api/paper/summary", method="DELETE"),
            ]
            for req in unauth_attacks:
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(req)
                assert exc_info.value.code == 405

            # Scenario B: Authenticated mutating attempts (even with valid token, mutation is rejected!)
            auth_attacks = [
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    data=b'{"cash": 1e6}',
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    method="POST",
                ),
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    data=b'{"symbol": "RELIANCE", "quantity": 1000}',
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    method="PUT",
                ),
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    headers={"Authorization": f"Bearer {token}"},
                    method="DELETE",
                ),
                urllib.request.Request(
                    f"{server.url}/api/paper/summary",
                    data=b'{"execution_locked": false}',
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    method="PATCH",
                ),
            ]
            for req in auth_attacks:
                with pytest.raises(urllib.error.HTTPError) as exc_info:
                    urllib.request.urlopen(req)
                assert exc_info.value.code == 405

            # Verify 100% financial and operational state invariance
            assert engine.portfolio.available_cash == initial_cash
            assert dict(engine.portfolio.positions) == initial_positions
            assert len(engine._orders) == initial_order_count
            assert len(engine._fills) == initial_fill_count
            assert len(engine.ledger.get_entries()) == initial_ledger_count

            # Core architectural safety invariants
            assert coordinator.settings.execution_locked is True
            assert coordinator.settings.broker_provider == "none"
            assert coordinator.state_store is not None
            assert coordinator.state_store.get("trading_engine") == "not initialized"
            assert coordinator.state_store.get("execution_engine") == "locked"
        finally:
            server.stop()


# ==============================================================================
# OBJECTIVE 6: STARTUP AND RUNTIME READINESS
# ==============================================================================


class TestStartupAndRuntimeReadiness:
    """Verifies startup lifecycle, shutdown cleanup, and socket safety."""

    def test_bootstrap_dashboard_disabled_does_not_start_listener(self) -> None:
        """bootstrap_system with dashboard_enabled=False leaves dashboard listener offline."""
        s = Settings(dashboard_enabled=False)
        status = bootstrap_system(settings=s, log_to_file=False)
        assert status.portfolio_dashboard_server is not None
        assert status.portfolio_dashboard_server.is_running is False

    def test_bootstrap_dashboard_enabled_starts_cleanly(self) -> None:
        """bootstrap_system with dashboard_enabled=True starts server cleanly on loopback."""
        s = Settings(dashboard_enabled=True, dashboard_port=0)
        status = bootstrap_system(settings=s, log_to_file=False)
        server = status.portfolio_dashboard_server
        assert server is not None
        assert server.is_running is True
        try:
            with urllib.request.urlopen(f"{server.url}/api/status") as resp:
                assert resp.status == 200
        finally:
            server.stop()

    def test_safe_server_shutdown_releases_resources(self) -> None:
        """Server stop() cleanly releases listening socket and terminates thread."""
        server = PortfolioDashboardServer(host="127.0.0.1", port=0)
        server.start(blocking=False)
        assert server.is_running is True
        assert server._server is not None
        assert server._thread is not None
        assert server._thread.is_alive()

        server.stop()
        assert server.is_running is False
        assert server._server is None
        assert server._thread is None


# ==============================================================================
# OBJECTIVE 7: TRADING ISOLATION REGRESSION
# ==============================================================================


class TestTradingIsolationRegression:
    """Verifies that all foundational trading safety boundaries remain 100% intact."""

    def test_trading_isolation_invariants(self) -> None:
        """Safety invariants remain locked and uninitialized across all components."""
        s = Settings()
        assert s.execution_locked is True
        assert s.paper_execution_enabled is False
        assert s.broker_provider == "none"
        assert s.paper_trading_only is True

        status = bootstrap_system(settings=s, log_to_file=False)
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"
        assert status.settings.execution_locked is True

        # State store verification
        store = status.state_store
        assert store is not None
        assert store.get("trading_engine") == "not initialized"
        assert store.get("execution_engine") == "locked"

    def test_voice_commands_cannot_bypass_execution_lock(self) -> None:
        """Voice command resolver strictly rejects live trading commands."""
        resolver = VoiceCommandIntentResolver()

        # Command attempting to execute a live trade
        result = resolver.resolve("buy 100 shares of reliance now")
        assert result.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND
        assert result.is_prohibited_trading is True
        assert result.rejection_reason is not None
        assert "strictly prohibited" in result.rejection_reason.lower()
