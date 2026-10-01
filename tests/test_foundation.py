"""KAISER — Foundation Layer Verification Test Suite.

Deterministic tests covering:
1. Configuration loading and validation
2. Exception hierarchy
3. Logging initialization and formatting
4. Application bootstrap and status invariants
5. Entry point (run.py) startup verification
"""

import io
import logging
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from app.bootstrap import SystemStatus, bootstrap_system, format_banner
from app.core.config import Settings, get_settings, load_dotenv, parse_bool
from app.core.exceptions import (
    ConfigurationError,
    ExecutionLockedError,
    KaiserError,
    RiskGateError,
    SecurityError,
    ServiceError,
    ValidationError,
)
from app.core.logging_config import get_logger, setup_logging
import os
import run


@pytest.fixture(autouse=True)
def isolate_env(monkeypatch):
    """Ensure environment is pristine for every test."""
    test_keys = [
        "APP_NAME",
        "APP_ENV",
        "LOG_LEVEL",
        "KAISER_LANGUAGE_MODE",
        "MARKET_DATA_PROVIDER",
        "BROKER_PROVIDER",
        "DATABASE_URL",
        "LLM_PROVIDER",
        "LLM_MODEL",
        "VOICE_ENABLED",
        "EXECUTION_LOCKED",
        "PAPER_TRADING_ONLY",
        "EMPTY_LINE",
    ]
    for key in test_keys:
        monkeypatch.delenv(key, raising=False)


# ==============================================================================
# 1. CONFIGURATION TESTS
# ==============================================================================

class TestConfiguration:
    """Test suite for configuration loading, parsing, and validation."""

    def test_default_settings(self) -> None:
        """Verify default settings values match foundation requirements."""
        settings = Settings()
        assert settings.app_name == "KAISER"
        assert settings.app_env == "development"
        assert settings.log_level == "INFO"
        assert settings.kaiser_language_mode == "tanglish"
        assert settings.market_data_provider == "mock"
        assert settings.broker_provider == "none"
        assert settings.database_url == "sqlite:///data/kaiser.db"
        assert settings.llm_provider == "mock"
        assert settings.llm_model == "mock-model"
        assert settings.voice_enabled is True
        assert settings.execution_locked is True
        assert settings.paper_trading_only is True

    def test_settings_from_custom_env(self) -> None:
        """Verify custom environment mapping properly overrides settings."""
        env_vars = {
            "APP_NAME": "KAISER_TEST",
            "APP_ENV": "staging",
            "LOG_LEVEL": "DEBUG",
            "KAISER_LANGUAGE_MODE": "english",
            "MARKET_DATA_PROVIDER": "custom_provider",
            "BROKER_PROVIDER": "paper",
            "DATABASE_URL": "sqlite:///test.db",
            "LLM_PROVIDER": "test_llm",
            "LLM_MODEL": "test_model_v1",
            "VOICE_ENABLED": "false",
            "EXECUTION_LOCKED": "true",
            "PAPER_TRADING_ONLY": "true",
        }
        settings = get_settings(env_source=env_vars)
        assert settings.app_name == "KAISER_TEST"
        assert settings.app_env == "staging"
        assert settings.log_level == "DEBUG"
        assert settings.kaiser_language_mode == "english"
        assert settings.market_data_provider == "custom_provider"
        assert settings.broker_provider == "paper"
        assert settings.database_url == "sqlite:///test.db"
        assert settings.llm_provider == "test_llm"
        assert settings.llm_model == "test_model_v1"
        assert settings.voice_enabled is False
        assert settings.execution_locked is True
        assert settings.paper_trading_only is True

    def test_settings_invalid_env_raises(self) -> None:
        """Verify an invalid APP_ENV raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            Settings(app_env="invalid_environment")
        assert "Invalid APP_ENV" in str(exc_info.value)

    def test_settings_invalid_log_level_raises(self) -> None:
        """Verify an invalid LOG_LEVEL raises ConfigurationError."""
        with pytest.raises(ConfigurationError) as exc_info:
            Settings(log_level="VERBOSE")
        assert "Invalid LOG_LEVEL" in str(exc_info.value)

    @pytest.mark.parametrize(
        "raw_value, expected",
        [
            ("true", True),
            ("True", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("off", False),
            (True, True),
            (False, False),
        ],
    )
    def test_parse_bool_valid(self, raw_value, expected) -> None:
        """Verify parse_bool accurately parses recognized true/false representations."""
        assert parse_bool(raw_value) is expected

    def test_parse_bool_invalid(self) -> None:
        """Verify parse_bool raises ConfigurationError on unrecognized values."""
        with pytest.raises(ConfigurationError) as exc_info:
            parse_bool("maybe", "TEST_FLAG")
        assert "Invalid boolean value for TEST_FLAG" in str(exc_info.value)

    def test_load_dotenv_file(self, tmp_path: Path) -> None:
        """Verify custom .env file loading with comments and quotes without mutating environ."""
        env_file = tmp_path / ".env.test"
        env_file.write_text(
            "# Test comment\n"
            "APP_NAME=KAISER_DOTENV\n"
            "LOG_LEVEL='DEBUG'\n"
            'VOICE_ENABLED="false"\n'
            "\n"
            "EMPTY_LINE=test_val\n",
            encoding="utf-8",
        )
        loaded = load_dotenv(dotenv_path=env_file, override=True, update_environ=False)
        assert loaded["APP_NAME"] == "KAISER_DOTENV"
        assert loaded["LOG_LEVEL"] == "DEBUG"
        assert loaded["VOICE_ENABLED"] == "false"
        assert loaded["EMPTY_LINE"] == "test_val"
        assert "APP_NAME" not in os.environ

    def test_load_dotenv_updates_environ(self, tmp_path: Path) -> None:
        """Verify load_dotenv populates os.environ when update_environ=True."""
        env_file = tmp_path / ".env.test2"
        env_file.write_text("APP_NAME=KAISER_UPDATED\n", encoding="utf-8")
        load_dotenv(dotenv_path=env_file, override=True, update_environ=True)
        assert os.environ["APP_NAME"] == "KAISER_UPDATED"

    def test_settings_to_dict(self) -> None:
        """Verify Settings.to_dict contains all expected keys."""
        settings = Settings()
        data = settings.to_dict()
        expected_keys = {
            "app_name",
            "app_env",
            "log_level",
            "kaiser_language_mode",
            "market_data_provider",
            "broker_provider",
            "database_url",
            "llm_provider",
            "llm_model",
            "voice_enabled",
            "execution_locked",
            "paper_trading_only",
        }
        assert expected_keys.issubset(data.keys())


# ==============================================================================
# 2. EXCEPTION HIERARCHY TESTS
# ==============================================================================

class TestExceptions:
    """Test suite verifying KAISER exception hierarchy."""

    def test_kaiser_error_base(self) -> None:
        """Verify KaiserError behaves as expected with message and details."""
        err = KaiserError("Base system error", details={"code": 500})
        assert str(err) == "Base system error (details: {'code': 500})"
        assert err.message == "Base system error"
        assert err.details == {"code": 500}

    @pytest.mark.parametrize(
        "exc_class",
        [
            ConfigurationError,
            ValidationError,
            ServiceError,
            SecurityError,
            ExecutionLockedError,
            RiskGateError,
        ],
    )
    def test_subclass_inheritance(self, exc_class) -> None:
        """Verify all domain exceptions inherit from KaiserError and Exception."""
        assert issubclass(exc_class, KaiserError)
        assert issubclass(exc_class, Exception)

    def test_catch_all_kaiser_errors(self) -> None:
        """Verify catching KaiserError catches all specialized domain exceptions."""
        exceptions_to_test = [
            ConfigurationError("Config failed"),
            ValidationError("Validation failed"),
            ServiceError("Service unavailable"),
            SecurityError("Unauthorized"),
            ExecutionLockedError("Execution locked"),
            RiskGateError("Risk limit exceeded"),
        ]
        for exc in exceptions_to_test:
            try:
                raise exc
            except KaiserError as caught:
                assert caught is exc


# ==============================================================================
# 3. LOGGING INITIALIZATION TESTS
# ==============================================================================

class TestLogging:
    """Test suite for centralized logging initialization."""

    def test_setup_logging_levels(self) -> None:
        """Verify logger sets appropriate numeric levels."""
        logger = setup_logging(log_level="DEBUG", log_to_file=False)
        assert logger.level == logging.DEBUG

        logger = setup_logging(log_level="WARNING", log_to_file=False)
        assert logger.level == logging.WARNING

    def test_get_logger_namespacing(self) -> None:
        """Verify get_logger returns logger in 'kaiser' namespace."""
        root_kaiser = get_logger()
        assert root_kaiser.name == "kaiser"

        sub_logger = get_logger("risk")
        assert sub_logger.name == "kaiser.risk"

        already_namespaced = get_logger("kaiser.agents")
        assert already_namespaced.name == "kaiser.agents"

    def test_setup_logging_idempotence(self) -> None:
        """Verify calling setup_logging multiple times does not duplicate handlers."""
        setup_logging(log_level="INFO", log_to_file=False)
        first_count = len(logging.getLogger("kaiser").handlers)

        setup_logging(log_level="INFO", log_to_file=False)
        second_count = len(logging.getLogger("kaiser").handlers)

        assert first_count == second_count == 1

    def test_file_logging_creation(self, tmp_path: Path) -> None:
        """Verify file logging writes to specified directory."""
        test_log_file = "test_run.log"
        logger = setup_logging(
            log_level="INFO",
            log_to_file=True,
            log_dir=tmp_path,
            log_file_name=test_log_file,
        )
        logger.info("Test log line for file verification.")

        # Flush handlers
        for handler in logger.handlers:
            handler.flush()

        log_path = tmp_path / test_log_file
        assert log_path.is_file()
        content = log_path.read_text(encoding="utf-8")
        assert "Test log line for file verification." in content
        assert "INFO" in content


# ==============================================================================
# 4. APPLICATION BOOTSTRAP TESTS
# ==============================================================================

class TestBootstrap:
    """Test suite for application initialization and safety invariants."""

    def test_bootstrap_system_default(self) -> None:
        """Verify bootstrap_system returns valid SystemStatus."""
        status = bootstrap_system(log_to_file=False)
        assert isinstance(status, SystemStatus)
        assert status.initialized is True
        assert status.app_name == "KAISER"
        assert status.environment == "development"
        assert status.voice_architecture == "ready"
        assert status.trading_engine == "not initialized"
        assert status.execution_engine == "locked"

    def test_safety_invariants_preserved(self) -> None:
        """Verify safety constraints are strictly maintained in foundation."""
        status = bootstrap_system(log_to_file=False)
        assert status.execution_engine == "locked"
        assert status.trading_engine == "not initialized"
        assert status.settings.paper_trading_only is True

    def test_voice_architecture_disabled_when_configured(self) -> None:
        """Verify voice architecture reports disabled if voice_enabled=False."""
        custom_settings = Settings(voice_enabled=False)
        status = bootstrap_system(settings=custom_settings, log_to_file=False)
        assert status.voice_architecture == "disabled"

    def test_format_banner(self) -> None:
        """Verify format_banner produces the required readable summary."""
        status = bootstrap_system(log_to_file=False)
        banner = format_banner(status)
        lines = [line.strip() for line in banner.splitlines()]

        assert lines[0] == "KAISER"
        assert lines[1] == "System initialized successfully."
        assert lines[2] == "Environment: development"
        assert lines[3] == "Voice-first architecture: ready"
        assert lines[4] == "Trading engine: not initialized"
        assert lines[5] == "Execution engine: locked"


# ==============================================================================
# 5. ENTRY POINT (RUN.PY) TESTS
# ==============================================================================

class TestRunEntryPoint:
    """Test suite for run.py entry point execution."""

    def test_run_main_in_process(self, capsys) -> None:
        """Test invoking run.main() directly in-process."""
        exit_code = run.main()
        assert exit_code == 0

        captured = capsys.readouterr()
        assert "KAISER" in captured.out
        assert "System initialized successfully." in captured.out
        assert "Execution engine: locked" in captured.out

    def test_run_main_handles_kaiser_error(self, capsys) -> None:
        """Verify run.main() cleanly reports KaiserError without traceback crash."""
        with patch("run.bootstrap_system", side_effect=ConfigurationError("Corrupt configuration")):
            exit_code = run.main()
            assert exit_code == 1

        captured = capsys.readouterr()
        assert "KAISER Configuration/Initialization Error: Corrupt configuration" in captured.err

    def test_run_py_subprocess(self) -> None:
        """Verify running `python run.py` as a standalone subprocess."""
        result = subprocess.run(
            [sys.executable, "run.py"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0
        output = result.stdout

        expected_elements = [
            "KAISER",
            "System initialized successfully.",
            "Environment: development",
            "Voice-first architecture: ready",
            "Trading engine: not initialized",
            "Execution engine: locked",
        ]
        for element in expected_elements:
            assert element in output
