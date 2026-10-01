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
    broker_provider: str = "none"
    database_url: str = "sqlite:///data/kaiser.db"
    llm_provider: str = "mock"
    llm_model: str = "mock-model"
    voice_enabled: bool = True

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

    def to_dict(self) -> dict[str, Any]:
        """Export settings as a dictionary."""
        return {
            "app_name": self.app_name,
            "app_env": self.app_env,
            "log_level": self.log_level,
            "kaiser_language_mode": self.kaiser_language_mode,
            "market_data_provider": self.market_data_provider,
            "broker_provider": self.broker_provider,
            "database_url": self.database_url,
            "llm_provider": self.llm_provider,
            "llm_model": self.llm_model,
            "voice_enabled": self.voice_enabled,
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

    return Settings(
        app_name=source.get("APP_NAME", "KAISER"),
        app_env=source.get("APP_ENV", "development"),
        log_level=source.get("LOG_LEVEL", "INFO"),
        kaiser_language_mode=source.get("KAISER_LANGUAGE_MODE", "tanglish"),
        market_data_provider=source.get("MARKET_DATA_PROVIDER", "mock"),
        broker_provider=source.get("BROKER_PROVIDER", "none"),
        database_url=source.get("DATABASE_URL", "sqlite:///data/kaiser.db"),
        llm_provider=source.get("LLM_PROVIDER", "mock"),
        llm_model=source.get("LLM_MODEL", "mock-model"),
        voice_enabled=parse_bool(source.get("VOICE_ENABLED", "true"), "VOICE_ENABLED"),
        execution_locked=parse_bool(source.get("EXECUTION_LOCKED", "true"), "EXECUTION_LOCKED"),
        paper_trading_only=parse_bool(source.get("PAPER_TRADING_ONLY", "true"), "PAPER_TRADING_ONLY"),
    )
