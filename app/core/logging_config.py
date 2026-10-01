"""KAISER — Centralized Logging Configuration.

Configures structured console and file logging with standardized timestamps,
levels, and module namespaces.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    log_level: str = "INFO",
    log_to_file: bool = True,
    log_dir: str | Path = "logs",
    log_file_name: str = "kaiser.log",
) -> logging.Logger:
    """Initialize and configure the root KAISER logger.

    Args:
        log_level: Desired log level string (e.g. DEBUG, INFO, WARNING, ERROR).
        log_to_file: Whether to attach a file logging handler.
        log_dir: Directory where log files are stored.
        log_file_name: Name of the log file.

    Returns:
        logging.Logger: The configured KAISER base logger.
    """
    numeric_level = getattr(logging, log_level.strip().upper(), logging.INFO)
    logger = logging.getLogger("kaiser")
    logger.setLevel(numeric_level)

    # Avoid duplicate handlers if setup_logging is invoked repeatedly
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)

    formatter = logging.Formatter(fmt=DEFAULT_LOG_FORMAT, datefmt=DEFAULT_DATE_FORMAT)

    # Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler
    if log_to_file:
        try:
            target_dir = Path(log_dir)
            target_dir.mkdir(parents=True, exist_ok=True)
            file_path = target_dir / log_file_name

            file_handler = logging.FileHandler(file_path, encoding="utf-8")
            file_handler.setLevel(numeric_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except OSError:
            # File logging failure shouldn't crash console logging or application bootstrap
            pass

    # Prevent propagation to the root logger to avoid double printing
    logger.propagate = False

    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Retrieve a namespaced KAISER logger.

    Args:
        name: Sub-module name (e.g., 'orchestrator', 'risk').

    Returns:
        logging.Logger: Logger instance under the 'kaiser' namespace.
    """
    if not name:
        return logging.getLogger("kaiser")
    if name.startswith("kaiser."):
        return logging.getLogger(name)
    return logging.getLogger(f"kaiser.{name}")
