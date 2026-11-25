"""KAISER — Configuration Management.

Handles environment-based configuration loading, typing, and validation.
Supports standard .env file loading with safe defaults.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from app.core.exceptions import ConfigurationError

VALID_ENVIRONMENTS = {"development", "test", "staging", "production"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
BOOLEAN_TRUE_STRINGS = {"true", "1", "yes", "on", "t"}
BOOLEAN_FALSE_STRINGS = {"false", "0", "no", "off", "f"}


def parse_bool(value: Any, field_name: str = "value") -> bool:
    """Safely parse a boolean from various representations.

    Args:
        value: Input value (bool, str, int).
        field_name: The name of the field being parsed for error context.

    Returns:
        bool: Parsed boolean value.

    Raises:
        ConfigurationError: If value cannot be interpreted as boolean.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in BOOLEAN_TRUE_STRINGS:
            return True
        if normalized in BOOLEAN_FALSE_STRINGS:
            return False
    raise ConfigurationError(
        f"Invalid boolean value for {field_name}: '{value}'. Expected one of: "
        f"{', '.join(sorted(BOOLEAN_TRUE_STRINGS | BOOLEAN_FALSE_STRINGS))}"
    )


def resolve_database_path(
    database_url_or_path: str = "sqlite:///data/kaiser.db",
    app_env: str = "development",
) -> str:
    """Resolve SQLite database path with robust test isolation guarantees.

    Ensures test execution never mutates, locks, or alters persistent user/production
    databases (such as 'data/kaiser.db'). If running within an automated test context
    (pytest detected, KAISER_TEST_DB_PATH set, or app_env == 'test') and the target
    resolves to the production default 'data/kaiser.db', this redirects the path to
    an isolated temporary database.
    """
    if not database_url_or_path:
        return database_url_or_path

    if database_url_or_path.startswith("sqlite:///"):
        raw_path = database_url_or_path[10:]
    elif database_url_or_path.startswith("sqlite://"):
        raw_path = database_url_or_path[9:]
    else:
        raw_path = database_url_or_path

    if raw_path == ":memory:":
        return raw_path

    # Check if raw_path points to the persistent production default database
    normalized = Path(raw_path).as_posix()
    is_prod_default = normalized in ("data/kaiser.db", "./data/kaiser.db", "kaiser.db")
    if not is_prod_default:
        try:
            prod_resolved = Path("data/kaiser.db").resolve()
            is_prod_default = Path(raw_path).resolve() == prod_resolved
        except Exception:
            is_prod_default = normalized.endswith("/data/kaiser.db")

    test_override = os.environ.get("KAISER_TEST_DB_PATH")
    import sys
    is_test_env = (
        bool(test_override)
        or bool(os.environ.get("PYTEST_CURRENT_TEST"))
        or "pytest" in sys.modules
        or app_env == "test"
    )

    if is_test_env and is_prod_default:
        if test_override:
            return test_override
        import tempfile
        import uuid
        return Path(tempfile.gettempdir(), f"kaiser_isolated_test_{os.getpid()}_{uuid.uuid4().hex[:12]}.db").as_posix()

    return raw_path


def load_dotenv(
    dotenv_path: str | Path | None = None,
    override: bool = False,
    update_environ: bool = True,
) -> dict[str, str]:
    """Parse a .env file and optionally populate os.environ.

    Pure standard library implementation to avoid hard third-party dependencies.

    Args:
        dotenv_path: Path to the .env file. Defaults to .env in current working dir.
        override: Whether existing os.environ values should be overwritten.
        update_environ: Whether to update os.environ with loaded keys.

    Returns:
        dict[str, str]: Key-value pairs loaded from the file.
    """
    path = Path(dotenv_path) if dotenv_path else Path(".env")
    loaded: dict[str, str] = {}

    if not path.is_file():
        return loaded

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as err:
        raise ConfigurationError(f"Failed to read .env file at {path}: {err}") from err

    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "=" not in line:
            # Skip invalid lines
            continue

        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()

        # Handle quoted values
        if len(val) >= 2 and (
            (val.startswith('"') and val.endswith('"')) or
            (val.startswith("'") and val.endswith("'"))
        ):
            val = val[1:-1]

        if key:
            loaded[key] = val
            if update_environ:
                if override or key not in os.environ:
                    os.environ[key] = val

    return loaded


@dataclass(frozen=True)
class Settings:
    """Immutable application settings container.

    All configurations default to safe, isolated local values suitable for
    development and unit testing.
    """

    app_name: str = "KAISER"
    app_env: str = "development"
    log_level: str = "INFO"
    kaiser_language_mode: str = "tanglish"
    market_data_provider: str = "mock"
    twelve_data_api_key: str | None = None
    twelve_data_symbols: str = "RELIANCE,TCS,INFY"
    twelve_data_exchange: str = "NSE"
    twelve_data_base_url: str = "https://api.twelvedata.com"
    twelve_data_rate_limit_per_minute: int = 8
    twelve_data_poll_interval_seconds: float = 15.0
    twelve_data_timeout_seconds: float = 10.0
    twelve_data_is_delayed: bool = True
    broker_provider: str = "none"
    database_url: str = "sqlite:///data/kaiser.db"
    llm_provider: str = "mock"
    llm_model: str = "mock-model"
    voice_enabled: bool = True
    stt_provider: str = "mock"
    stt_model_path: str | None = None
    stt_model_id: str = "whisper-base"
    stt_device: str = "cpu"
    stt_compute_type: str = "int8"
    tts_provider: str = "mock"
    tts_voice_id: str | None = None
    tts_rate: int = 175
    tts_volume: float = 1.0
    playback_enabled: bool = False
    playback_device_id: int | None = None

    # Portfolio Analytics Dashboard UI & Access Control (Phase 2, Step 12 & Step 56)
    dashboard_enabled: bool = False
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8080
    dashboard_auth_token: str | None = None
    dashboard_require_auth: bool = False

    # Paper Execution & Simulated Broker (Phase 2, Step 53)
    paper_execution_enabled: bool = False
    paper_initial_cash: float = 100000.0
    paper_max_order_notional: float = 50000.0
    paper_max_position_exposure_pct: float = 50.0
    paper_fee_rate: float = 0.0005
    paper_slippage_rate: float = 0.0002
    paper_freshness_threshold_seconds: float = 60.0

    # Durable Transactional Outbox (Phase 2, Step 64)
    outbox_enabled: bool = True
    outbox_max_attempts: int = 5
    outbox_batch_size: int = 50
    outbox_retry_backoff_seconds: float = 2.0
    outbox_max_backoff_seconds: float = 60.0
    outbox_auto_dispatch: bool = False

    # Outbox Retention & Pruning (Phase 2, Step 65)
    outbox_retention_enabled: bool = False
    outbox_retention_days: float = 7.0
    outbox_prune_batch_size: int = 100
    outbox_archival_enabled: bool = False

    # Automated Maintenance Scheduler (Phase 2, Step 66)
    maintenance_scheduler_enabled: bool = False
    maintenance_pruning_enabled: bool = False
    maintenance_pruning_interval_seconds: float = 3600.0
    maintenance_health_digest_enabled: bool = False
    maintenance_health_digest_interval_seconds: float = 300.0
    maintenance_active_session_policy: str = "skip"
    maintenance_shutdown_timeout_seconds: float = 5.0
    maintenance_history_limit: int = 1000
    maintenance_consecutive_failure_threshold: int = 3
    maintenance_duration_sample_limit: int = 100

    # Database Backup & Disaster Recovery (Phase 2, Steps 78 & 79)
    backup_dir: str = "data/backups"
    backup_retention_enabled: bool = False
    backup_retention_max_count: int = 5
    backup_scheduler_enabled: bool = False
    backup_interval_seconds: float = 3600.0
    backup_health_max_age_seconds: float = 86400.0
    backup_retention_max_age_days: float = 30.0
    backup_prune_batch_size: int = 50
    backup_retry_max_attempts: int = 3
    backup_shutdown_timeout_seconds: float = 5.0
    recovery_drill_enabled: bool = False

    # Architectural Safety Invariants
    execution_locked: bool = True
    paper_trading_only: bool = True

    def __post_init__(self) -> None:
        """Validate settings after initialization."""
        norm_env = self.app_env.strip().lower()
        if norm_env not in VALID_ENVIRONMENTS:
            raise ConfigurationError(
                f"Invalid APP_ENV: '{self.app_env}'. Must be one of: "
                f"{', '.join(sorted(VALID_ENVIRONMENTS))}"
            )
        object.__setattr__(self, "app_env", norm_env)

        norm_log = self.log_level.strip().upper()
        if norm_log not in VALID_LOG_LEVELS:
            raise ConfigurationError(
                f"Invalid LOG_LEVEL: '{self.log_level}'. Must be one of: "
                f"{', '.join(sorted(VALID_LOG_LEVELS))}"
            )
        object.__setattr__(self, "log_level", norm_log)

        norm_mdp = self.market_data_provider.strip().lower()
        object.__setattr__(self, "market_data_provider", norm_mdp)
        if norm_mdp == "twelvedata":
            if not self.twelve_data_api_key or not self.twelve_data_api_key.strip():
                raise ConfigurationError(
                    "TWELVE_DATA_API_KEY must be configured when MARKET_DATA_PROVIDER is 'twelvedata'. "
                    "Safe offline default is MARKET_DATA_PROVIDER='mock'."
                )

        norm_host = self.dashboard_host.strip().lower()
        if norm_host not in {"127.0.0.1", "localhost", "::1"}:
            raise ConfigurationError(
                f"Invalid DASHBOARD_HOST: '{self.dashboard_host}'. Dashboard must bind strictly "
                f"to loopback interfaces ('127.0.0.1', 'localhost', '::1')."
            )
        object.__setattr__(self, "dashboard_host", norm_host)

        # Boolean field validation & normalization
        for bool_field in (
            "dashboard_enabled",
            "dashboard_require_auth",
            "paper_execution_enabled",
            "execution_locked",
            "paper_trading_only",
            "voice_enabled",
            "playback_enabled",
            "twelve_data_is_delayed",
            "outbox_enabled",
            "outbox_auto_dispatch",
            "outbox_retention_enabled",
            "outbox_archival_enabled",
        ):
            val = getattr(self, bool_field)
            if not isinstance(val, bool):
                parsed = parse_bool(val, bool_field.upper())
                object.__setattr__(self, bool_field, parsed)

        if isinstance(self.dashboard_port, str):
            try:
                object.__setattr__(self, "dashboard_port", int(self.dashboard_port.strip()))
            except ValueError:
                raise ConfigurationError(
                    f"Invalid DASHBOARD_PORT: '{self.dashboard_port}'. Port must be an integer between 0 and 65535."
                )

        if not isinstance(self.dashboard_port, int) or self.dashboard_port < 0 or self.dashboard_port > 65535:
            raise ConfigurationError(
                f"Invalid DASHBOARD_PORT: {self.dashboard_port}. Port must be an integer between 0 and 65535."
            )

        if self.dashboard_port == 0 and self.app_env.strip().lower() == "production":
            raise ConfigurationError(
                "Invalid DASHBOARD_PORT: 0. Ephemeral port binding (port 0) is reserved for test/development environments and is prohibited in production."
            )

        if self.dashboard_auth_token is not None:
            if not isinstance(self.dashboard_auth_token, str):
                raise ConfigurationError(
                    f"Invalid DASHBOARD_AUTH_TOKEN: expected string or None, got {type(self.dashboard_auth_token).__name__}."
                )
            clean_tok = self.dashboard_auth_token.strip()
            object.__setattr__(self, "dashboard_auth_token", clean_tok if clean_tok else None)

        if self.twelve_data_api_key is not None and not isinstance(self.twelve_data_api_key, str):
            raise ConfigurationError(
                f"Invalid TWELVE_DATA_API_KEY: expected string or None, got {type(self.twelve_data_api_key).__name__}."
            )

        if self.paper_initial_cash <= 0:
            raise ConfigurationError(
                f"Invalid PAPER_INITIAL_CASH: {self.paper_initial_cash}. Initial cash must be positive."
            )
        if self.paper_max_order_notional <= 0:
            raise ConfigurationError(
                f"Invalid PAPER_MAX_ORDER_NOTIONAL: {self.paper_max_order_notional}. Max notional must be positive."
            )
        if self.paper_max_position_exposure_pct <= 0 or self.paper_max_position_exposure_pct > 100:
            raise ConfigurationError(
                f"Invalid PAPER_MAX_POSITION_EXPOSURE_PCT: {self.paper_max_position_exposure_pct}. "
                "Must be greater than 0 and at most 100."
            )
        if self.paper_fee_rate < 0:
            raise ConfigurationError(
                f"Invalid PAPER_FEE_RATE: {self.paper_fee_rate}. Fee rate cannot be negative."
            )
        if self.paper_slippage_rate < 0:
            raise ConfigurationError(
                f"Invalid PAPER_SLIPPAGE_RATE: {self.paper_slippage_rate}. Slippage rate cannot be negative."
            )
        if self.paper_freshness_threshold_seconds <= 0:
            raise ConfigurationError(
                f"Invalid PAPER_FRESHNESS_THRESHOLD_SECONDS: {self.paper_freshness_threshold_seconds}. Must be positive."
            )

        # Durable Transactional Outbox Validation
        if self.outbox_max_attempts <= 0:
            raise ConfigurationError(
                f"Invalid OUTBOX_MAX_ATTEMPTS: {self.outbox_max_attempts}. Must be a positive integer."
            )
        if self.outbox_batch_size <= 0:
            raise ConfigurationError(
                f"Invalid OUTBOX_BATCH_SIZE: {self.outbox_batch_size}. Must be a positive integer."
            )
        if self.outbox_retry_backoff_seconds < 0:
            raise ConfigurationError(
                f"Invalid OUTBOX_RETRY_BACKOFF_SECONDS: {self.outbox_retry_backoff_seconds}. Must be non-negative."
            )
        if self.outbox_max_backoff_seconds < self.outbox_retry_backoff_seconds:
            raise ConfigurationError(
                f"Invalid OUTBOX_MAX_BACKOFF_SECONDS: {self.outbox_max_backoff_seconds}. "
                f"Must be >= OUTBOX_RETRY_BACKOFF_SECONDS ({self.outbox_retry_backoff_seconds})."
            )

        # Outbox Retention Validation (Phase 2, Step 65)
        if not math.isfinite(self.outbox_retention_days) or self.outbox_retention_days <= 0:
            raise ConfigurationError(
                f"Invalid OUTBOX_RETENTION_DAYS: {self.outbox_retention_days}. Must be a positive finite float."
            )
        if self.outbox_prune_batch_size <= 0 or self.outbox_prune_batch_size > 50_000:
            raise ConfigurationError(
                f"Invalid OUTBOX_PRUNE_BATCH_SIZE: {self.outbox_prune_batch_size}. Must be an integer between 1 and 50000."
            )

        # Automated Maintenance Scheduler Validation (Phase 2, Step 66)
        if not math.isfinite(self.maintenance_pruning_interval_seconds) or self.maintenance_pruning_interval_seconds <= 0 or self.maintenance_pruning_interval_seconds > 86400.0 * 30:
            raise ConfigurationError(
                f"Invalid MAINTENANCE_PRUNING_INTERVAL_SECONDS: {self.maintenance_pruning_interval_seconds}. Must be a positive finite float <= 2592000 (30 days)."
            )
        if not math.isfinite(self.maintenance_health_digest_interval_seconds) or self.maintenance_health_digest_interval_seconds <= 0 or self.maintenance_health_digest_interval_seconds > 86400.0 * 7:
            raise ConfigurationError(
                f"Invalid MAINTENANCE_HEALTH_DIGEST_INTERVAL_SECONDS: {self.maintenance_health_digest_interval_seconds}. Must be a positive finite float <= 604800 (7 days)."
            )
        if not math.isfinite(self.maintenance_shutdown_timeout_seconds) or self.maintenance_shutdown_timeout_seconds <= 0 or self.maintenance_shutdown_timeout_seconds > 60.0:
            raise ConfigurationError(
                f"Invalid MAINTENANCE_SHUTDOWN_TIMEOUT_SECONDS: {self.maintenance_shutdown_timeout_seconds}. Must be a positive finite float <= 60.0."
            )
        if self.maintenance_history_limit < 10 or self.maintenance_history_limit > 100_000:
            raise ConfigurationError(
                f"Invalid MAINTENANCE_HISTORY_LIMIT: {self.maintenance_history_limit}. Must be an integer between 10 and 100000."
            )
        if self.maintenance_consecutive_failure_threshold < 1 or self.maintenance_consecutive_failure_threshold > 1000:
            raise ConfigurationError(
                f"Invalid MAINTENANCE_CONSECUTIVE_FAILURE_THRESHOLD: {self.maintenance_consecutive_failure_threshold}. Must be an integer between 1 and 1000."
            )
        if self.maintenance_duration_sample_limit < 10 or self.maintenance_duration_sample_limit > 10_000:
            raise ConfigurationError(
                f"Invalid MAINTENANCE_DURATION_SAMPLE_LIMIT: {self.maintenance_duration_sample_limit}. Must be an integer between 10 and 10000."
            )
        norm_policy = str(self.maintenance_active_session_policy).strip().lower()
        if norm_policy not in ("skip", "allow"):
            raise ConfigurationError(
                f"Invalid MAINTENANCE_ACTIVE_SESSION_POLICY: '{self.maintenance_active_session_policy}'. Must be 'skip' or 'allow'."
            )
        object.__setattr__(self, "maintenance_active_session_policy", norm_policy)

        if self.backup_retention_max_count < 1 or self.backup_retention_max_count > 10_000:
            raise ConfigurationError(
                f"Invalid BACKUP_RETENTION_MAX_COUNT: {self.backup_retention_max_count}. Must be an integer between 1 and 10000."
            )
        if self.backup_interval_seconds <= 0 or not math.isfinite(self.backup_interval_seconds):
            raise ConfigurationError(
                f"Invalid BACKUP_INTERVAL_SECONDS: {self.backup_interval_seconds}. Must be a positive finite float."
            )
        if self.backup_health_max_age_seconds <= 0 or not math.isfinite(self.backup_health_max_age_seconds):
            raise ConfigurationError(
                f"Invalid BACKUP_HEALTH_MAX_AGE_SECONDS: {self.backup_health_max_age_seconds}. Must be a positive finite float."
            )
        if self.backup_retention_max_age_days <= 0 or not math.isfinite(self.backup_retention_max_age_days):
            raise ConfigurationError(
                f"Invalid BACKUP_RETENTION_MAX_AGE_DAYS: {self.backup_retention_max_age_days}. Must be a positive finite float."
            )
        if self.backup_prune_batch_size < 1 or self.backup_prune_batch_size > 10_000:
            raise ConfigurationError(
                f"Invalid BACKUP_PRUNE_BATCH_SIZE: {self.backup_prune_batch_size}. Must be an integer between 1 and 10000."
            )
        if self.backup_retry_max_attempts < 0 or self.backup_retry_max_attempts > 10:
            raise ConfigurationError(
                f"Invalid BACKUP_RETRY_MAX_ATTEMPTS: {self.backup_retry_max_attempts}. Must be an integer between 0 and 10."
            )
        if self.backup_shutdown_timeout_seconds <= 0 or self.backup_shutdown_timeout_seconds > 60.0 or not math.isfinite(self.backup_shutdown_timeout_seconds):
            raise ConfigurationError(
                f"Invalid BACKUP_SHUTDOWN_TIMEOUT_SECONDS: {self.backup_shutdown_timeout_seconds}. Must be a positive finite float <= 60.0."
            )

        # Broker provider validation (must remain 'none' or 'paper' in test contexts)
        norm_broker = self.broker_provider.strip().lower()
        if norm_broker not in {"none", "paper"}:
            raise ConfigurationError(
                f"Unsupported BROKER_PROVIDER: '{self.broker_provider}'. Live external broker integration is "
                f"strictly prohibited in Phase 2. Allowed providers are: 'none', 'paper'."
            )
        object.__setattr__(self, "broker_provider", norm_broker)

        # Database URL validation (must be valid SQLite URL or :memory:)
        if not self.database_url or not isinstance(self.database_url, str) or not self.database_url.strip():
            raise ConfigurationError("DATABASE_URL cannot be empty. Must specify a valid SQLite connection URL.")
        clean_db_url = self.database_url.strip()
        if not (clean_db_url.startswith("sqlite://") or clean_db_url == ":memory:"):
            raise ConfigurationError(
                f"Unsupported database scheme in DATABASE_URL: '{self.database_url}'. "
                f"KAISER only supports SQLite databases (e.g. 'sqlite:///data/kaiser.db' or ':memory:')."
            )
        object.__setattr__(self, "database_url", clean_db_url)

        # Backup directory validation
        if not self.backup_dir or not isinstance(self.backup_dir, str) or not self.backup_dir.strip():
            raise ConfigurationError("BACKUP_DIR cannot be empty. Must specify a valid backup directory path.")

        # Speech-to-Text provider validation
        norm_stt = self.stt_provider.strip().lower()
        if norm_stt not in {"mock", "local_whisper"}:
            raise ConfigurationError(
                f"Unsupported STT_PROVIDER: '{self.stt_provider}'. Allowed providers are: 'mock', 'local_whisper'."
            )
        object.__setattr__(self, "stt_provider", norm_stt)

        # Text-to-Speech provider validation
        norm_tts = self.tts_provider.strip().lower()
        if norm_tts not in {"mock", "pyttsx3", "local_pyttsx3"}:
            raise ConfigurationError(
                f"Unsupported TTS_PROVIDER: '{self.tts_provider}'. Allowed providers are: 'mock', 'pyttsx3', 'local_pyttsx3'."
            )
        object.__setattr__(self, "tts_provider", norm_tts)

    def to_dict(self) -> dict[str, Any]:
        """Export settings as a dictionary."""
        return {
            "app_name": self.app_name,
            "app_env": self.app_env,
            "log_level": self.log_level,
            "kaiser_language_mode": self.kaiser_language_mode,
            "market_data_provider": self.market_data_provider,
            "twelve_data_api_key": "***" if self.twelve_data_api_key else None,
            "twelve_data_symbols": self.twelve_data_symbols,
            "twelve_data_exchange": self.twelve_data_exchange,
            "twelve_data_base_url": self.twelve_data_base_url,
            "twelve_data_rate_limit_per_minute": self.twelve_data_rate_limit_per_minute,
            "twelve_data_poll_interval_seconds": self.twelve_data_poll_interval_seconds,
            "twelve_data_timeout_seconds": self.twelve_data_timeout_seconds,
            "twelve_data_is_delayed": self.twelve_data_is_delayed,
            "broker_provider": self.broker_provider,
            "database_url": self.database_url,
            "llm_provider": self.llm_provider,
            "llm_model": self.llm_model,
            "voice_enabled": self.voice_enabled,
            "stt_provider": self.stt_provider,
            "stt_model_path": self.stt_model_path,
            "stt_model_id": self.stt_model_id,
            "stt_device": self.stt_device,
            "stt_compute_type": self.stt_compute_type,
            "tts_provider": self.tts_provider,
            "tts_voice_id": self.tts_voice_id,
            "tts_rate": self.tts_rate,
            "tts_volume": self.tts_volume,
            "playback_enabled": self.playback_enabled,
            "playback_device_id": self.playback_device_id,
            "dashboard_enabled": self.dashboard_enabled,
            "dashboard_host": self.dashboard_host,
            "dashboard_port": self.dashboard_port,
            "dashboard_auth_token": "***" if self.dashboard_auth_token else None,
            "dashboard_require_auth": self.dashboard_require_auth,
            "paper_execution_enabled": self.paper_execution_enabled,
            "paper_initial_cash": self.paper_initial_cash,
            "paper_max_order_notional": self.paper_max_order_notional,
            "paper_max_position_exposure_pct": self.paper_max_position_exposure_pct,
            "paper_fee_rate": self.paper_fee_rate,
            "paper_slippage_rate": self.paper_slippage_rate,
            "paper_freshness_threshold_seconds": self.paper_freshness_threshold_seconds,
            "outbox_enabled": self.outbox_enabled,
            "outbox_max_attempts": self.outbox_max_attempts,
            "outbox_batch_size": self.outbox_batch_size,
            "outbox_retry_backoff_seconds": self.outbox_retry_backoff_seconds,
            "outbox_max_backoff_seconds": self.outbox_max_backoff_seconds,
            "outbox_auto_dispatch": self.outbox_auto_dispatch,
            "outbox_retention_enabled": self.outbox_retention_enabled,
            "outbox_retention_days": self.outbox_retention_days,
            "outbox_prune_batch_size": self.outbox_prune_batch_size,
            "outbox_archival_enabled": self.outbox_archival_enabled,
            "maintenance_scheduler_enabled": self.maintenance_scheduler_enabled,
            "maintenance_pruning_enabled": self.maintenance_pruning_enabled,
            "maintenance_pruning_interval_seconds": self.maintenance_pruning_interval_seconds,
            "maintenance_health_digest_enabled": self.maintenance_health_digest_enabled,
            "maintenance_health_digest_interval_seconds": self.maintenance_health_digest_interval_seconds,
            "maintenance_active_session_policy": self.maintenance_active_session_policy,
            "maintenance_shutdown_timeout_seconds": self.maintenance_shutdown_timeout_seconds,
            "maintenance_history_limit": self.maintenance_history_limit,
            "maintenance_consecutive_failure_threshold": self.maintenance_consecutive_failure_threshold,
            "maintenance_duration_sample_limit": self.maintenance_duration_sample_limit,
            "backup_dir": self.backup_dir,
            "backup_retention_enabled": self.backup_retention_enabled,
            "backup_retention_max_count": self.backup_retention_max_count,
            "backup_scheduler_enabled": self.backup_scheduler_enabled,
            "backup_interval_seconds": self.backup_interval_seconds,
            "backup_health_max_age_seconds": self.backup_health_max_age_seconds,
            "backup_retention_max_age_days": self.backup_retention_max_age_days,
            "backup_prune_batch_size": self.backup_prune_batch_size,
            "backup_retry_max_attempts": self.backup_retry_max_attempts,
            "backup_shutdown_timeout_seconds": self.backup_shutdown_timeout_seconds,
            "recovery_drill_enabled": self.recovery_drill_enabled,
            "execution_locked": self.execution_locked,
            "paper_trading_only": self.paper_trading_only,
        }


def get_settings(
    env_source: Mapping[str, str] | None = None,
    dotenv_path: str | Path | None = None,
) -> Settings:
    """Factory to create Settings from environment variables.

    Args:
        env_source: Optional dictionary of environment variables.
                    Defaults to reading os.environ after loading .env.
        dotenv_path: Path to optional .env file to load into environment.

    Returns:
        Settings: Validated configuration instance.
    """
    if env_source is None:
        load_dotenv(dotenv_path=dotenv_path)
        source = os.environ
    else:
        source = env_source

    try:
        rate_limit = int(source.get("TWELVE_DATA_RATE_LIMIT_PER_MINUTE", "8"))
        poll_interval = float(source.get("TWELVE_DATA_POLL_INTERVAL_SECONDS", "15.0"))
        timeout = float(source.get("TWELVE_DATA_TIMEOUT_SECONDS", "10.0"))
        tts_rate = int(source.get("TTS_RATE", "175"))
        tts_volume = float(source.get("TTS_VOLUME", "1.0"))
        raw_device_id = source.get("PLAYBACK_DEVICE_ID", "").strip() if "PLAYBACK_DEVICE_ID" in source else ""
        playback_device_id = int(raw_device_id) if raw_device_id else None
        paper_initial_cash = float(source.get("PAPER_INITIAL_CASH", "100000.0"))
        paper_max_order_notional = float(source.get("PAPER_MAX_ORDER_NOTIONAL", "50000.0"))
        paper_max_position_exposure_pct = float(source.get("PAPER_MAX_POSITION_EXPOSURE_PCT", "50.0"))
        paper_fee_rate = float(source.get("PAPER_FEE_RATE", "0.0005"))
        paper_slippage_rate = float(source.get("PAPER_SLIPPAGE_RATE", "0.0002"))
        paper_freshness = float(source.get("PAPER_FRESHNESS_THRESHOLD_SECONDS", "60.0"))

        outbox_max_attempts = int(source.get("OUTBOX_MAX_ATTEMPTS", "5"))
        outbox_batch_size = int(source.get("OUTBOX_BATCH_SIZE", "50"))
        outbox_retry_backoff = float(source.get("OUTBOX_RETRY_BACKOFF_SECONDS", "2.0"))
        outbox_max_backoff = float(source.get("OUTBOX_MAX_BACKOFF_SECONDS", "60.0"))
        outbox_retention_days = float(source.get("OUTBOX_RETENTION_DAYS", "7.0"))
        outbox_prune_batch_size = int(source.get("OUTBOX_PRUNE_BATCH_SIZE", "100"))
        maint_prune_interval = float(source.get("MAINTENANCE_PRUNING_INTERVAL_SECONDS", "3600.0"))
        maint_digest_interval = float(source.get("MAINTENANCE_HEALTH_DIGEST_INTERVAL_SECONDS", "300.0"))
        maint_shutdown_timeout = float(source.get("MAINTENANCE_SHUTDOWN_TIMEOUT_SECONDS", "5.0"))
        maint_history_limit = int(source.get("MAINTENANCE_HISTORY_LIMIT", "1000"))
        maint_failure_threshold = int(source.get("MAINTENANCE_CONSECUTIVE_FAILURE_THRESHOLD", "3"))
        maint_sample_limit = int(source.get("MAINTENANCE_DURATION_SAMPLE_LIMIT", "100"))
        backup_retention_max_count = int(source.get("BACKUP_RETENTION_MAX_COUNT", "5"))
        backup_interval = float(source.get("BACKUP_INTERVAL_SECONDS", "3600.0"))
        backup_health_max_age = float(source.get("BACKUP_HEALTH_MAX_AGE_SECONDS", "86400.0"))
        backup_retention_max_age = float(source.get("BACKUP_RETENTION_MAX_AGE_DAYS", "30.0"))
        backup_prune_batch = int(source.get("BACKUP_PRUNE_BATCH_SIZE", "50"))
        backup_retry_max = int(source.get("BACKUP_RETRY_MAX_ATTEMPTS", "3"))
        backup_shutdown_timeout = float(source.get("BACKUP_SHUTDOWN_TIMEOUT_SECONDS", "5.0"))
    except ValueError as val_err:
        raise ConfigurationError(f"Invalid numeric configuration in environment: {val_err}") from val_err

    return Settings(
        app_name=source.get("APP_NAME", "KAISER"),
        app_env=source.get("APP_ENV", "development"),
        log_level=source.get("LOG_LEVEL", "INFO"),
        kaiser_language_mode=source.get("KAISER_LANGUAGE_MODE", "tanglish"),
        market_data_provider=source.get("MARKET_DATA_PROVIDER", "mock"),
        twelve_data_api_key=source.get("TWELVE_DATA_API_KEY", None),
        twelve_data_symbols=source.get("TWELVE_DATA_SYMBOLS", "RELIANCE,TCS,INFY"),
        twelve_data_exchange=source.get("TWELVE_DATA_EXCHANGE", "NSE"),
        twelve_data_base_url=source.get("TWELVE_DATA_BASE_URL", "https://api.twelvedata.com"),
        twelve_data_rate_limit_per_minute=rate_limit,
        twelve_data_poll_interval_seconds=poll_interval,
        twelve_data_timeout_seconds=timeout,
        twelve_data_is_delayed=parse_bool(source.get("TWELVE_DATA_IS_DELAYED", "true"), "TWELVE_DATA_IS_DELAYED"),
        broker_provider=source.get("BROKER_PROVIDER", "none"),
        database_url=source.get("DATABASE_URL", "sqlite:///data/kaiser.db"),
        llm_provider=source.get("LLM_PROVIDER", "mock"),
        llm_model=source.get("LLM_MODEL", "mock-model"),
        voice_enabled=parse_bool(source.get("VOICE_ENABLED", "true"), "VOICE_ENABLED"),
        stt_provider=source.get("STT_PROVIDER", "mock"),
        stt_model_path=source.get("STT_MODEL_PATH", None),
        stt_model_id=source.get("STT_MODEL_ID", "whisper-base"),
        stt_device=source.get("STT_DEVICE", "cpu"),
        stt_compute_type=source.get("STT_COMPUTE_TYPE", "int8"),
        tts_provider=source.get("TTS_PROVIDER", "mock"),
        tts_voice_id=source.get("TTS_VOICE_ID", None),
        tts_rate=tts_rate,
        tts_volume=tts_volume,
        playback_enabled=parse_bool(source.get("PLAYBACK_ENABLED", "false"), "PLAYBACK_ENABLED"),
        playback_device_id=playback_device_id,
        dashboard_enabled=parse_bool(source.get("DASHBOARD_ENABLED", "false"), "DASHBOARD_ENABLED"),
        dashboard_host=source.get("DASHBOARD_HOST", "127.0.0.1"),
        dashboard_port=source.get("DASHBOARD_PORT", 8080),  # type: ignore[arg-type]
        dashboard_auth_token=source.get("DASHBOARD_AUTH_TOKEN", None),
        dashboard_require_auth=parse_bool(source.get("DASHBOARD_REQUIRE_AUTH", "false"), "DASHBOARD_REQUIRE_AUTH"),
        paper_execution_enabled=parse_bool(source.get("PAPER_EXECUTION_ENABLED", "false"), "PAPER_EXECUTION_ENABLED"),
        paper_initial_cash=paper_initial_cash,
        paper_max_order_notional=paper_max_order_notional,
        paper_max_position_exposure_pct=paper_max_position_exposure_pct,
        paper_fee_rate=paper_fee_rate,
        paper_slippage_rate=paper_slippage_rate,
        paper_freshness_threshold_seconds=paper_freshness,
        outbox_enabled=parse_bool(source.get("OUTBOX_ENABLED", "true"), "OUTBOX_ENABLED"),
        outbox_max_attempts=outbox_max_attempts,
        outbox_batch_size=outbox_batch_size,
        outbox_retry_backoff_seconds=outbox_retry_backoff,
        outbox_max_backoff_seconds=outbox_max_backoff,
        outbox_auto_dispatch=parse_bool(source.get("OUTBOX_AUTO_DISPATCH", "false"), "OUTBOX_AUTO_DISPATCH"),
        outbox_retention_enabled=parse_bool(source.get("OUTBOX_RETENTION_ENABLED", "false"), "OUTBOX_RETENTION_ENABLED"),
        outbox_retention_days=outbox_retention_days,
        outbox_prune_batch_size=outbox_prune_batch_size,
        outbox_archival_enabled=parse_bool(source.get("OUTBOX_ARCHIVAL_ENABLED", "false"), "OUTBOX_ARCHIVAL_ENABLED"),
        maintenance_scheduler_enabled=parse_bool(source.get("MAINTENANCE_SCHEDULER_ENABLED", "false"), "MAINTENANCE_SCHEDULER_ENABLED"),
        maintenance_pruning_enabled=parse_bool(source.get("MAINTENANCE_PRUNING_ENABLED", "false"), "MAINTENANCE_PRUNING_ENABLED"),
        maintenance_pruning_interval_seconds=maint_prune_interval,
        maintenance_health_digest_enabled=parse_bool(source.get("MAINTENANCE_HEALTH_DIGEST_ENABLED", "false"), "MAINTENANCE_HEALTH_DIGEST_ENABLED"),
        maintenance_health_digest_interval_seconds=maint_digest_interval,
        maintenance_active_session_policy=source.get("MAINTENANCE_ACTIVE_SESSION_POLICY", "skip"),
        maintenance_shutdown_timeout_seconds=maint_shutdown_timeout,
        maintenance_history_limit=maint_history_limit,
        maintenance_consecutive_failure_threshold=maint_failure_threshold,
        maintenance_duration_sample_limit=maint_sample_limit,
        backup_dir=source.get("BACKUP_DIR", "data/backups"),
        backup_retention_enabled=parse_bool(source.get("BACKUP_RETENTION_ENABLED", "false"), "BACKUP_RETENTION_ENABLED"),
        backup_retention_max_count=backup_retention_max_count,
        backup_scheduler_enabled=parse_bool(source.get("BACKUP_SCHEDULER_ENABLED", "false"), "BACKUP_SCHEDULER_ENABLED"),
        backup_interval_seconds=backup_interval,
        backup_health_max_age_seconds=backup_health_max_age,
        backup_retention_max_age_days=backup_retention_max_age,
        backup_prune_batch_size=backup_prune_batch,
        backup_retry_max_attempts=backup_retry_max,
        backup_shutdown_timeout_seconds=backup_shutdown_timeout,
        recovery_drill_enabled=parse_bool(source.get("RECOVERY_DRILL_ENABLED", "false"), "RECOVERY_DRILL_ENABLED"),
        execution_locked=parse_bool(source.get("EXECUTION_LOCKED", "true"), "EXECUTION_LOCKED"),
        paper_trading_only=parse_bool(source.get("PAPER_TRADING_ONLY", "true"), "PAPER_TRADING_ONLY"),
    )

