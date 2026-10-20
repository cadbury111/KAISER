"""KAISER — Configuration Management.

Handles environment-based configuration loading, typing, and validation.
Supports standard .env file loading with safe defaults.
"""

from __future__ import annotations

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
        execution_locked=parse_bool(source.get("EXECUTION_LOCKED", "true"), "EXECUTION_LOCKED"),
        paper_trading_only=parse_bool(source.get("PAPER_TRADING_ONLY", "true"), "PAPER_TRADING_ONLY"),
    )
