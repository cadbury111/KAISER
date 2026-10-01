"""KAISER — Core Exception Hierarchy.

Provides a clean, domain-specific exception hierarchy for error handling
across the KAISER platform.
"""


class KaiserError(Exception):
    """Base exception for all KAISER errors."""

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} (details: {self.details})"
        return self.message


class ConfigurationError(KaiserError):
    """Raised when application configuration is missing, invalid, or malformed."""


class ValidationError(KaiserError):
    """Raised when input parameters, schemas, or data fail validation checks."""


class ServiceError(KaiserError):
    """Raised when an internal service or subsystem fails to operate correctly."""


class SecurityError(KaiserError):
    """Raised when security boundaries, secrets, or authorization rules are violated."""


class ExecutionLockedError(KaiserError):
    """Raised when order execution is attempted while the execution engine is locked.

    By architectural invariant, execution is locked by default and requires
    independent risk clearance and human approval.
    """


class RiskGateError(KaiserError):
    """Raised when a risk limit or safety invariant is violated."""
