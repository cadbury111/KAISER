"""KAISER — Historical Review Audit Observability, Alerting & Operational Diagnostics Models.

Defines immutable, structured diagnostic records, categories, severity levels,
evidence classifications, and operational alert models for historical review audit
and reliability workflows.

SAFETY & RESEARCH INVARIANTS:
1. Purely informational, research-only operational diagnostics.
2. Clear separation between OBSERVED_FAILURE, INFERRED_INTERPRETATION, and HUMAN_DECISION.
3. Prohibits trading recommendations, win probabilities, profit predictions, or strategy rankings.
4. Bounded in-memory retention and deterministic correlation identifiers.
5. Secret-safe: credentials, tokens, paths, raw audio, and prompts are scrubbed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Dict, List, Optional, Tuple, Union

from app.core.exceptions import ValidationError

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")

PROHIBITED_WORDS = {
    "buy",
    "sell",
    "target_price",
    "win_probability",
    "profit_prediction",
    "strategy_ranking",
    "order_placement",
    "place_order",
    "broker_integration",
}

CREDENTIAL_KEY_REGEX = re.compile(
    r"(?i)(api[_-]?key|secret|password|private[_-]?key|auth[_-]?token|bearer|credential|token)"
)
CREDENTIAL_VALUE_REGEX = re.compile(
    r"(?i)(api[_-]?key|secret|password|token|bearer|credential)\s*[:=]\s*['\"]?([A-Za-z0-9_\-\.\$\/+=]{3,})['\"]?"
)
TOKEN_PATTERNS = re.compile(
    r"(?:ghp_[A-Za-z0-9]{36}|ey[A-Za-z0-9_-]{15,}\.ey[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]{15,}|[A-Fa-f0-9]{32,64})"
)
WINDOWS_PATH_REGEX = re.compile(r"[A-Za-z]:\\[^\s\"'<>]+", re.IGNORECASE)
UNIX_PATH_REGEX = re.compile(r"/(?:Users|home|root|var|etc|tmp|opt|usr)/[^\s\"'<>]+", re.IGNORECASE)


def format_utc_iso(epoch_seconds: Optional[float]) -> str:
    """Format an epoch timestamp to a standardized UTC ISO-8601 string."""
    if (
        epoch_seconds is None
        or not isinstance(epoch_seconds, (int, float))
        or math.isnan(epoch_seconds)
        or math.isinf(epoch_seconds)
        or epoch_seconds <= 0.0
    ):
        return "1970-01-01T00:00:00Z"
    try:
        dt = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return "1970-01-01T00:00:00Z"


def sanitize_text(text: Any, max_length: int = 512) -> str:
    """Sanitize strings by scrubbing paths, credentials, and control characters."""
    if text is None:
        return ""
    s = str(text).replace("\x00", " ")
    s = WINDOWS_PATH_REGEX.sub("[REDACTED_PATH]", s)
    s = UNIX_PATH_REGEX.sub("[REDACTED_PATH]", s)
    s = CREDENTIAL_VALUE_REGEX.sub(r"\1=[REDACTED_SECRET]", s)
    s = TOKEN_PATTERNS.sub("[REDACTED_TOKEN]", s)
    s = CREDENTIAL_KEY_REGEX.sub("[REDACTED_KEY]", s)
    if len(s) > max_length:
        s = s[: max_length - 3] + "..."
    return s


def validate_safe_id(val: str, field_name: str) -> str:
    """Validate that an identifier is clean and safe."""
    clean = (val or "").strip()
    if not clean or not SAFE_ID_PATTERN.match(clean) or ".." in clean or len(clean) > 128:
        raise ValidationError(f"Invalid {field_name} format: '{val}'. Must be a safe identifier (<=128 chars, no '..').")
    return clean


def check_prohibited_terms(text: str, context: str) -> None:
    """Ensure no prohibited financial execution or ranking terms are present."""
    if not text:
        return
    lower = text.lower()
    for word in PROHIBITED_WORDS:
        if re.search(r"\b" + re.escape(word) + r"\b", lower):
            raise ValidationError(
                f"Prohibited trading or ranking term '{word}' detected in {context}. "
                "KAISER historical review diagnostics must remain strictly informational."
            )


class AuditDiagnosticCategory(str, Enum):
    """Categorized operational diagnostic event types."""

    AUDIT_EVALUATION_FAILURE = "AUDIT_EVALUATION_FAILURE"
    COVERAGE_DEGRADATION = "COVERAGE_DEGRADATION"
    REPEATABILITY_DIVERGENCE = "REPEATABILITY_DIVERGENCE"
    HISTORICAL_INTEGRITY_REGRESSION = "HISTORICAL_INTEGRITY_REGRESSION"
    INTEGRITY_VIOLATION = "INTEGRITY_VIOLATION"
    RECOVERY_OBSERVED = "RECOVERY_OBSERVED"
    GENERAL_DIAGNOSTIC = "GENERAL_DIAGNOSTIC"


class AuditDiagnosticSeverity(str, Enum):
    """Standardized operational severity classifications."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AuditEvidenceNature(str, Enum):
    """Strict separation of diagnostic evidence nature."""

    OBSERVED_FAILURE = "OBSERVED_FAILURE"
    INFERRED_INTERPRETATION = "INFERRED_INTERPRETATION"
    HUMAN_DECISION = "HUMAN_DECISION"


class AuditAlertStatus(str, Enum):
    """Operational alert state."""

    TRIGGERED = "TRIGGERED"
    ACTIVE = "ACTIVE"
    SUPPRESSED_COOLDOWN = "SUPPRESSED_COOLDOWN"
    RECOVERED = "RECOVERED"
    RESOLVED = "RESOLVED"
    ACKNOWLEDGED = "ACKNOWLEDGED"


def compute_deterministic_diagnostic_id(
    investigation_id: str,
    category: Union[AuditDiagnosticCategory, str],
    timestamp: Optional[float],
    correlation_id: str,
) -> str:
    """Generate a deterministic diagnostic ID."""
    cat_val = category.value if isinstance(category, Enum) else str(category)
    safe_ts = (
        0.0
        if (
            timestamp is None
            or not isinstance(timestamp, (int, float))
            or math.isnan(timestamp)
            or math.isinf(timestamp)
        )
        else float(timestamp)
    )
    seed = f"{investigation_id}:{cat_val}:{safe_ts:.4f}:{correlation_id}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"diag_{digest}"


def compute_deterministic_alert_id(
    investigation_id: str,
    category: Union[AuditDiagnosticCategory, str],
    timestamp: Optional[float],
    reason: str,
) -> str:
    """Generate a deterministic alert ID."""
    cat_val = category.value if isinstance(category, Enum) else str(category)
    safe_ts = (
        0.0
        if (
            timestamp is None
            or not isinstance(timestamp, (int, float))
            or math.isnan(timestamp)
            or math.isinf(timestamp)
        )
        else float(timestamp)
    )
    seed = f"{investigation_id}:{cat_val}:{safe_ts:.4f}:{reason[:64]}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]
    return f"alert_{digest}"


@dataclass(frozen=True)
class AuditDiagnosticRecord:
    """Strongly typed, immutable operational diagnostic record."""

    diagnostic_id: str
    correlation_id: str
    investigation_id: str
    timestamp: float
    timestamp_iso: str
    category: AuditDiagnosticCategory
    severity: AuditDiagnosticSeverity
    evidence_nature: AuditEvidenceNature
    message: str
    report_id: Optional[str] = None
    evaluation_id: Optional[str] = None
    source_references: Tuple[str, ...] = field(default_factory=tuple)
    details: Dict[str, Any] = field(default_factory=dict)
    error_category: Optional[str] = None
    sanitized_summary: str = ""

    def __post_init__(self) -> None:
        validate_safe_id(self.diagnostic_id, "diagnostic_id")
        validate_safe_id(self.correlation_id, "correlation_id")
        validate_safe_id(self.investigation_id, "investigation_id")
        if self.report_id:
            validate_safe_id(self.report_id, "report_id")
        if self.evaluation_id:
            validate_safe_id(self.evaluation_id, "evaluation_id")
        check_prohibited_terms(self.message, "AuditDiagnosticRecord.message")
        check_prohibited_terms(self.sanitized_summary, "AuditDiagnosticRecord.sanitized_summary")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize record to dictionary."""
        return {
            "diagnostic_id": self.diagnostic_id,
            "correlation_id": self.correlation_id,
            "investigation_id": self.investigation_id,
            "timestamp": self.timestamp,
            "timestamp_iso": self.timestamp_iso,
            "category": self.category.value,
            "severity": self.severity.value,
            "evidence_nature": self.evidence_nature.value,
            "message": self.message,
            "report_id": self.report_id,
            "evaluation_id": self.evaluation_id,
            "source_references": list(self.source_references),
            "details": self.details,
            "error_category": self.error_category,
            "sanitized_summary": self.sanitized_summary,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditDiagnosticRecord:
        """Deserialize from dictionary."""
        cat_raw = data.get("category", AuditDiagnosticCategory.GENERAL_DIAGNOSTIC.value)
        try:
            category = AuditDiagnosticCategory(cat_raw)
        except ValueError:
            category = AuditDiagnosticCategory.GENERAL_DIAGNOSTIC

        sev_raw = data.get("severity", AuditDiagnosticSeverity.INFO.value)
        try:
            severity = AuditDiagnosticSeverity(sev_raw)
        except ValueError:
            severity = AuditDiagnosticSeverity.INFO

        nat_raw = data.get("evidence_nature", AuditEvidenceNature.OBSERVED_FAILURE.value)
        try:
            nature = AuditEvidenceNature(nat_raw)
        except ValueError:
            nature = AuditEvidenceNature.OBSERVED_FAILURE

        ts = float(data.get("timestamp", 0.0))
        return cls(
            diagnostic_id=str(data["diagnostic_id"]),
            correlation_id=str(data["correlation_id"]),
            investigation_id=str(data["investigation_id"]),
            timestamp=ts,
            timestamp_iso=str(data.get("timestamp_iso", format_utc_iso(ts))),
            category=category,
            severity=severity,
            evidence_nature=nature,
            message=str(data.get("message", "")),
            report_id=data.get("report_id"),
            evaluation_id=data.get("evaluation_id"),
            source_references=tuple(str(s) for s in data.get("source_references", ())),
            details=dict(data.get("details", {})),
            error_category=data.get("error_category"),
            sanitized_summary=str(data.get("sanitized_summary", "")),
        )


@dataclass(frozen=True)
class AuditAlert:
    """Strongly typed, immutable operational alert."""

    alert_id: str
    diagnostic_id: str
    correlation_id: str
    investigation_id: str
    timestamp: float
    timestamp_iso: str
    category: AuditDiagnosticCategory
    severity: AuditDiagnosticSeverity
    reason: str
    source_references: Tuple[str, ...] = field(default_factory=tuple)
    status: AuditAlertStatus = AuditAlertStatus.TRIGGERED
    cooldown_until: float = 0.0
    suppressed_occurrences: int = 0
    report_id: Optional[str] = None
    evaluation_id: Optional[str] = None

    def __post_init__(self) -> None:
        validate_safe_id(self.alert_id, "alert_id")
        validate_safe_id(self.diagnostic_id, "diagnostic_id")
        validate_safe_id(self.correlation_id, "correlation_id")
        validate_safe_id(self.investigation_id, "investigation_id")
        if self.report_id:
            validate_safe_id(self.report_id, "report_id")
        if self.evaluation_id:
            validate_safe_id(self.evaluation_id, "evaluation_id")
        check_prohibited_terms(self.reason, "AuditAlert.reason")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize alert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "diagnostic_id": self.diagnostic_id,
            "correlation_id": self.correlation_id,
            "investigation_id": self.investigation_id,
            "timestamp": self.timestamp,
            "timestamp_iso": self.timestamp_iso,
            "category": self.category.value,
            "severity": self.severity.value,
            "reason": self.reason,
            "source_references": list(self.source_references),
            "status": self.status.value,
            "cooldown_until": self.cooldown_until,
            "suppressed_occurrences": self.suppressed_occurrences,
            "report_id": self.report_id,
            "evaluation_id": self.evaluation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditAlert:
        """Deserialize alert from dictionary."""
        cat_raw = data.get("category", AuditDiagnosticCategory.GENERAL_DIAGNOSTIC.value)
        try:
            category = AuditDiagnosticCategory(cat_raw)
        except ValueError:
            category = AuditDiagnosticCategory.GENERAL_DIAGNOSTIC

        sev_raw = data.get("severity", AuditDiagnosticSeverity.WARNING.value)
        try:
            severity = AuditDiagnosticSeverity(sev_raw)
        except ValueError:
            severity = AuditDiagnosticSeverity.WARNING

        stat_raw = data.get("status", AuditAlertStatus.TRIGGERED.value)
        try:
            status = AuditAlertStatus(stat_raw)
        except ValueError:
            status = AuditAlertStatus.TRIGGERED

        ts = float(data.get("timestamp", 0.0))
        return cls(
            alert_id=str(data["alert_id"]),
            diagnostic_id=str(data["diagnostic_id"]),
            correlation_id=str(data["correlation_id"]),
            investigation_id=str(data["investigation_id"]),
            timestamp=ts,
            timestamp_iso=str(data.get("timestamp_iso", format_utc_iso(ts))),
            category=category,
            severity=severity,
            reason=str(data.get("reason", "")),
            source_references=tuple(str(s) for s in data.get("source_references", ())),
            status=status,
            cooldown_until=float(data.get("cooldown_until", 0.0)),
            suppressed_occurrences=int(data.get("suppressed_occurrences", 0)),
            report_id=data.get("report_id"),
            evaluation_id=data.get("evaluation_id"),
        )


@dataclass(frozen=True)
class AuditDiagnosticSummary:
    """Sanitized diagnostic roll-up summary for an investigation."""

    investigation_id: str
    total_diagnostics: int
    total_alerts: int
    active_alerts_count: int
    category_counts: Dict[str, int]
    severity_counts: Dict[str, int]
    evidence_nature_counts: Dict[str, int]
    latest_timestamp: Optional[float] = None
    latest_timestamp_iso: Optional[str] = None
    has_active_failures: bool = False
    recovery_observed: bool = False
    recent_alert_reasons: Tuple[str, ...] = field(default_factory=tuple)
    retention_status: str = "COMPLETE"
    is_truncated: bool = False
    evicted_diagnostics_count: int = 0
    evicted_alerts_count: int = 0
    oldest_timestamp: Optional[float] = None
    oldest_timestamp_iso: Optional[str] = None
    capacity: int = 100

    def __post_init__(self) -> None:
        validate_safe_id(self.investigation_id, "investigation_id")

    def to_dict(self) -> Dict[str, Any]:
        """Serialize summary to dictionary."""
        return {
            "investigation_id": self.investigation_id,
            "total_diagnostics": self.total_diagnostics,
            "total_alerts": self.total_alerts,
            "active_alerts_count": self.active_alerts_count,
            "category_counts": self.category_counts,
            "severity_counts": self.severity_counts,
            "evidence_nature_counts": self.evidence_nature_counts,
            "latest_timestamp": self.latest_timestamp,
            "latest_timestamp_iso": self.latest_timestamp_iso,
            "has_active_failures": self.has_active_failures,
            "recovery_observed": self.recovery_observed,
            "recent_alert_reasons": list(self.recent_alert_reasons),
            "retention_status": self.retention_status,
            "is_truncated": self.is_truncated,
            "evicted_diagnostics_count": self.evicted_diagnostics_count,
            "evicted_alerts_count": self.evicted_alerts_count,
            "oldest_timestamp": self.oldest_timestamp,
            "oldest_timestamp_iso": self.oldest_timestamp_iso,
            "capacity": self.capacity,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditDiagnosticSummary:
        """Deserialize summary from dictionary."""
        return cls(
            investigation_id=str(data["investigation_id"]),
            total_diagnostics=int(data.get("total_diagnostics", 0)),
            total_alerts=int(data.get("total_alerts", 0)),
            active_alerts_count=int(data.get("active_alerts_count", 0)),
            category_counts=dict(data.get("category_counts", {})),
            severity_counts=dict(data.get("severity_counts", {})),
            evidence_nature_counts=dict(data.get("evidence_nature_counts", {})),
            latest_timestamp=data.get("latest_timestamp"),
            latest_timestamp_iso=data.get("latest_timestamp_iso"),
            has_active_failures=bool(data.get("has_active_failures", False)),
            recovery_observed=bool(data.get("recovery_observed", False)),
            recent_alert_reasons=tuple(str(r) for r in data.get("recent_alert_reasons", ())),
            retention_status=str(data.get("retention_status", "COMPLETE")),
            is_truncated=bool(data.get("is_truncated", False)),
            evicted_diagnostics_count=int(data.get("evicted_diagnostics_count", 0)),
            evicted_alerts_count=int(data.get("evicted_alerts_count", 0)),
            oldest_timestamp=data.get("oldest_timestamp"),
            oldest_timestamp_iso=data.get("oldest_timestamp_iso"),
            capacity=int(data.get("capacity", 100)),
        )
