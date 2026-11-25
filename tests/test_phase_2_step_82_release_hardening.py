"""KAISER — Phase 2, Step 82: Paper Trading Release Hardening & Deployment Readiness Tests.

Comprehensive regression and verification suite covering:
1. Spoken order-intent conversational boundary hardening ("place an order for reliance").
2. Preservation of legitimate analytical voice queries.
3. Unknown intent safety (fails closed, non-executable).
4. Configuration validation & safe defaults (broker_provider, database_url, stt/tts, backup_dir).
5. Graceful system shutdown (shutdown_system lifecycle, idempotency, partial failure resilience).
6. Dashboard operational security and granular runtime health reporting (/api/status, /api/health).
7. Repeated startup/shutdown cycles without resource leakage.
8. Real database immutability verification.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict

import pytest

from app.bootstrap import SystemStatus, bootstrap_system, shutdown_system
from app.core.config import Settings, get_settings
from app.core.exceptions import ConfigurationError
from app.core.events import EventBus
from app.core.state import StateStore
from app.portfolio import PortfolioReportingService, PortfolioDashboardServer
from app.voice.intent import VoiceCommandIntentResolver, VoiceIntentType


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    """Provide an isolated temporary test directory."""
    return tmp_path


@pytest.fixture
def isolated_db_path(temp_dir: Path) -> str:
    """Provide an isolated temporary SQLite database path."""
    return str(temp_dir / "step82_isolated.db")


# ==============================================================================
# 1. VOICE INTENT CONVERSATIONAL BOUNDARY HARDENING
# ==============================================================================


class TestStep82VoiceIntentSafety:
    """Verify that conversational variants of order placement are strictly intercepted."""

    def test_voice_intent_conversational_order_variants_blocked(self) -> None:
        """Conversational order directives with articles resolve to PROHIBITED_TRADING_COMMAND."""
        resolver = VoiceCommandIntentResolver()

        conversational_order_phrases = [
            "place an order for reliance",
            "place the order for tcs",
            "place order for infosys",
            "execute an order now",
            "execute the trade for reliance",
            "send an order for reliance",
            "send the order immediately",
            "market order for reliance",
            "limit order for tcs",
            "reliance vangiko",
            "tcs vaangiko",
            "order podu reliance ku",
            "trade execute pannu now",
        ]

        for phrase in conversational_order_phrases:
            intent = resolver.resolve(phrase)
            assert intent.intent_type == VoiceIntentType.PROHIBITED_TRADING_COMMAND, (
                f"Failed to block conversational trading directive: '{phrase}' (got {intent.intent_type})"
            )
            assert intent.is_prohibited_trading is True
            assert intent.confidence == 1.0

    def test_voice_intent_preserves_legitimate_analytical_commands(self) -> None:
        """Legitimate market analysis and system status commands remain properly routed."""
        resolver = VoiceCommandIntentResolver()

        analytical_phrases = [
            ("system status", VoiceIntentType.SYSTEM_STATUS),
            ("how is the system", VoiceIntentType.SYSTEM_STATUS),
            ("portfolio risk status", VoiceIntentType.PORTFOLIO_RISK_STATUS),
            ("market data quality", VoiceIntentType.MARKET_DATA_QUALITY),
            ("show opportunities", VoiceIntentType.OPPORTUNITY_PRIORITIES),
            ("investigation queue", VoiceIntentType.INVESTIGATION_QUEUE),
            ("latest portfolio report", VoiceIntentType.LATEST_PORTFOLIO_REPORT),
            ("analyze reliance", VoiceIntentType.TECHNICAL_ANALYSIS),
            ("technical analysis for tcs", VoiceIntentType.TECHNICAL_ANALYSIS),
            ("consensus for reliance", VoiceIntentType.MULTI_AGENT_CONSENSUS),
            ("coordinate agent consensus for infy", VoiceIntentType.MULTI_AGENT_CONSENSUS),
        ]

        for phrase, expected_type in analytical_phrases:
            intent = resolver.resolve(phrase)
            assert intent.intent_type == expected_type, (
                f"Legitimate phrase '{phrase}' misclassified as {intent.intent_type}"
            )
            assert intent.is_prohibited_trading is False

    def test_voice_intent_unknown_queries_remain_non_executable(self) -> None:
        """Non-trading unknown queries resolve to UNKNOWN and have confidence 0.0."""
        resolver = VoiceCommandIntentResolver()

        unknown_phrases = [
            "what is the weather today",
            "tell me a joke",
            "open the door",
            "who is the president",
        ]

        for phrase in unknown_phrases:
            intent = resolver.resolve(phrase)
            assert intent.intent_type == VoiceIntentType.UNKNOWN
            assert intent.confidence == 0.0
            assert intent.is_prohibited_trading is False


# ==============================================================================
# 2. CONFIGURATION VALIDATION & SAFE DEFAULTS
# ==============================================================================


class TestStep82ConfigurationValidation:
    """Verify strict fail-closed configuration validation."""

    def test_config_rejects_unsupported_broker_provider(self) -> None:
        """Attempting to set an external live broker provider raises ConfigurationError."""
        for invalid_broker in ("zerodha", "interactive_brokers", "alpaca", "binance"):
            with pytest.raises(ConfigurationError, match="Unsupported BROKER_PROVIDER"):
                Settings(broker_provider=invalid_broker)

        # 'none' and 'paper' must remain allowed
        s_none = Settings(broker_provider="none")
        assert s_none.broker_provider == "none"
        s_paper = Settings(broker_provider="paper")
        assert s_paper.broker_provider == "paper"

    def test_config_rejects_empty_or_unsupported_database_urls(self) -> None:
        """Empty or non-SQLite database URLs raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="DATABASE_URL cannot be empty"):
            Settings(database_url="")

        with pytest.raises(ConfigurationError, match="Unsupported database scheme"):
            Settings(database_url="postgresql://user:pass@localhost:5432/kaiser")

        with pytest.raises(ConfigurationError, match="Unsupported database scheme"):
            Settings(database_url="mysql://user:pass@localhost/kaiser")

        s_valid = Settings(database_url="sqlite:///data/test.db")
        assert s_valid.database_url == "sqlite:///data/test.db"
        s_mem = Settings(database_url=":memory:")
        assert s_mem.database_url == ":memory:"

    def test_config_rejects_empty_backup_dir(self) -> None:
        """Empty BACKUP_DIR raises ConfigurationError."""
        with pytest.raises(ConfigurationError, match="BACKUP_DIR cannot be empty"):
            Settings(backup_dir="")

    def test_config_rejects_invalid_stt_tts_providers(self) -> None:
        """Unsupported STT or TTS providers raise ConfigurationError."""
        with pytest.raises(ConfigurationError, match="Unsupported STT_PROVIDER"):
            Settings(stt_provider="cloud_google")

        with pytest.raises(ConfigurationError, match="Unsupported TTS_PROVIDER"):
            Settings(tts_provider="elevenlabs")

        s = Settings(stt_provider="mock", tts_provider="mock")
        assert s.stt_provider == "mock"
        assert s.tts_provider == "mock"

    def test_config_secrets_redacted_in_dict_export(self) -> None:
        """Exported configuration dictionaries strictly redact API keys and tokens."""
        s = Settings(
            twelve_data_api_key="secret_twelve_key_999",
            dashboard_auth_token="secret_auth_token_888",
        )
        d = s.to_dict()
        assert d["twelve_data_api_key"] == "***"
        assert d["dashboard_auth_token"] == "***"


# ==============================================================================
# 3. SAFE STARTUP & GRACEFUL SHUTDOWN LIFECYCLE
# ==============================================================================


class TestStep82LifecycleAndShutdown:
    """Verify application startup, component ordering, and graceful shutdown."""

    def test_shutdown_system_graceful_and_idempotent(self, isolated_db_path: str) -> None:
        """shutdown_system cleanly releases resources and is safe to call multiple times."""
        settings = Settings(
            app_env="test",
            database_url=f"sqlite:///{isolated_db_path}",
            voice_enabled=True,
            dashboard_enabled=False,
            paper_execution_enabled=False,
        )
        status = bootstrap_system(settings=settings)
        assert status.initialized is True
        assert status.state_store.get("system_status") == "initialized"

        # First shutdown call
        clean = shutdown_system(status)
        assert clean is True
        assert status.state_store.get("system_status") == "shutdown"
        assert status.state_store.get("shutdown_clean") is True
        assert status.event_bus.is_shutdown is True

        # Second shutdown call (idempotent no-op)
        clean_again = shutdown_system(status)
        assert clean_again is True

        # None status safe no-op
        assert shutdown_system(None) is True

    def test_shutdown_system_handles_partial_startup_state(self) -> None:
        """shutdown_system handles SystemStatus with None subsystems without raising."""
        dummy_event_bus = EventBus(name="dummy_bus")
        dummy_state_store = StateStore(name="dummy_state")
        settings = Settings(app_env="test")

        partial_status = SystemStatus(
            app_name="KAISER",
            environment="test",
            voice_architecture="disabled",
            trading_engine="not initialized",
            execution_engine="locked",
            initialized=False,
            settings=settings,
            event_bus=dummy_event_bus,
            state_store=dummy_state_store,
            portfolio_dashboard_server=None,
            voice_controller=None,
            market_service=None,
        )

        clean = shutdown_system(partial_status)
        assert clean is True
        assert dummy_event_bus.is_shutdown is True
        assert dummy_state_store.get("system_status") == "shutdown"

    def test_repeated_bootstrap_and_shutdown_cycles(self, temp_dir: Path) -> None:
        """Repeatedly bootstrapping and shutting down does not leak state or fail."""
        for cycle in range(3):
            cycle_db = str(temp_dir / f"cycle_{cycle}.db")
            s = Settings(
                app_env="test",
                database_url=f"sqlite:///{cycle_db}",
                voice_enabled=True,
                dashboard_enabled=False,
            )
            status = bootstrap_system(settings=s)
            assert status.initialized is True
            clean = shutdown_system(status)
            assert clean is True


# ==============================================================================
# 4. DASHBOARD HEALTH & OBSERVABILITY HARDENING
# ==============================================================================


class TestStep82DashboardObservability:
    """Verify granular runtime health reporting and authorization boundaries."""

    def test_api_status_and_health_return_granular_truthful_subsystems(
        self,
        temp_dir: Path,
    ) -> None:
        """GET /api/status and /api/health report detailed subsystem health without false claims."""
        reporting_service = PortfolioReportingService()
        server = PortfolioDashboardServer(
            reporting_service=reporting_service,
            host="127.0.0.1",
            port=0,
            require_auth=False,
        )
        server.start(blocking=False)
        try:
            for endpoint in ("/api/status", "/api/health"):
                req = urllib.request.Request(f"{server.url}{endpoint}")
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    assert resp.status == 200
                    import json
                    data = json.loads(resp.read().decode("utf-8"))
                    assert data["status"] == "healthy"
                    assert data["app"] == "KAISER"
                    assert data["execution_locked"] is True
                    assert data["broker_provider"] == "none"
                    assert data["trading_engine"] == "not initialized"
                    assert data["voice_microphone_capture"] is False
                    assert data["market_data_live_connected"] is False
                    assert "subsystems" in data
                    subsystems = data["subsystems"]
                    assert subsystems["execution_engine"] == "locked"
                    assert subsystems["broker_interface"] == "none"
                    assert subsystems["voice_subsystem"] == "safe_offline"
        finally:
            server.stop()

    def test_api_status_and_health_accessible_when_auth_required(self) -> None:
        """Public health endpoints remain accessible when require_auth=True without leaking data."""
        reporting_service = PortfolioReportingService()
        server = PortfolioDashboardServer(
            reporting_service=reporting_service,
            host="127.0.0.1",
            port=0,
            auth_token="super_secret_token_abc",
            require_auth=True,
        )
        server.start(blocking=False)
        try:
            # Public health endpoints succeed without credentials
            for endpoint in ("/api/status", "/api/health"):
                req = urllib.request.Request(f"{server.url}{endpoint}")
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    assert resp.status == 200

            # Sensitive endpoints require authentication
            req_sens = urllib.request.Request(f"{server.url}/api/report/latest")
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                urllib.request.urlopen(req_sens, timeout=3.0)
            assert exc_info.value.code == 401
        finally:
            server.stop()


# ==============================================================================
# 5. CLI HELP & DATABASE IMMUTABILITY VERIFICATION
# ==============================================================================


class TestStep82CliAndDatabaseImmutability:
    """Verify CLI entry point help and persistent database integrity."""

    def test_run_py_help_smoke(self) -> None:
        """python run.py --help executes cleanly with exit code 0."""
        res = subprocess.run(
            [sys.executable, "run.py", "--help"],
            cwd=str(Path(__file__).parent.parent),
            capture_output=True,
            text=True,
        )
        assert res.returncode == 0
        assert "KAISER — Voice-First AI Trading Desk" in res.stdout
        assert "--dashboard" in res.stdout

    def test_real_kaiser_db_hash_immutability(self) -> None:
        """Ensure real data/kaiser.db has not been altered."""
        real_db = Path("data/kaiser.db")
        if real_db.exists():
            h = hashlib.sha256(real_db.read_bytes()).hexdigest().upper()
            assert h == "C142CFD5FD9920E74187C10FB0C47636FF3AE49B2320CF06A95B4F7D4AAFCD4B"
            assert real_db.stat().st_size == 335872
