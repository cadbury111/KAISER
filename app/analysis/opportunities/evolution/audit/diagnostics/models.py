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
from app.analysis.opportunities.evolution.audit.diagnostics.schema import (
    CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS,
    LEGACY_UNVERSIONED,
    SchemaVersionCompatibilityStatus,
    SchemaVersionError,
    IncompatibleSchemaVersionError,
    MalformedSchemaVersionError,
    detect_and_validate_schema_version,
    migrate_diagnostic_payload,
    migrate_alert_payload,
    migrate_export_payload,
    migrate_reconciliation_payload,
)

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
    schema_version: str = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

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
        status, _ = detect_and_validate_schema_version(self.schema_version)
        if status == SchemaVersionCompatibilityStatus.MALFORMED:
            raise MalformedSchemaVersionError(f"Malformed schema_version: '{self.schema_version}'", detected_version=self.schema_version)
        if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
            raise IncompatibleSchemaVersionError(f"Unsupported schema_version: '{self.schema_version}'", detected_version=self.schema_version)

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
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditDiagnosticRecord:
        """Deserialize from dictionary with schema version migration."""
        migrated_data = migrate_diagnostic_payload(data)
        cat_raw = migrated_data.get("category", AuditDiagnosticCategory.GENERAL_DIAGNOSTIC.value)
        try:
            category = AuditDiagnosticCategory(cat_raw)
        except ValueError:
            category = AuditDiagnosticCategory.GENERAL_DIAGNOSTIC

        sev_raw = migrated_data.get("severity", AuditDiagnosticSeverity.INFO.value)
        try:
            severity = AuditDiagnosticSeverity(sev_raw)
        except ValueError:
            severity = AuditDiagnosticSeverity.INFO

        nat_raw = migrated_data.get("evidence_nature", AuditEvidenceNature.OBSERVED_FAILURE.value)
        try:
            nature = AuditEvidenceNature(nat_raw)
        except ValueError:
            nature = AuditEvidenceNature.OBSERVED_FAILURE

        ts = float(migrated_data.get("timestamp", 0.0))
        return cls(
            diagnostic_id=str(migrated_data["diagnostic_id"]),
            correlation_id=str(migrated_data["correlation_id"]),
            investigation_id=str(migrated_data["investigation_id"]),
            timestamp=ts,
            timestamp_iso=str(migrated_data.get("timestamp_iso", format_utc_iso(ts))),
            category=category,
            severity=severity,
            evidence_nature=nature,
            message=str(migrated_data.get("message", "")),
            report_id=migrated_data.get("report_id"),
            evaluation_id=migrated_data.get("evaluation_id"),
            source_references=tuple(str(s) for s in migrated_data.get("source_references", ())),
            details=dict(migrated_data.get("details", {})),
            error_category=migrated_data.get("error_category"),
            sanitized_summary=str(migrated_data.get("sanitized_summary", "")),
            schema_version=str(migrated_data.get("schema_version", CURRENT_DIAGNOSTICS_SCHEMA_VERSION)),
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
    schema_version: str = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

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
        status, _ = detect_and_validate_schema_version(self.schema_version)
        if status == SchemaVersionCompatibilityStatus.MALFORMED:
            raise MalformedSchemaVersionError(f"Malformed schema_version: '{self.schema_version}'", detected_version=self.schema_version)
        if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
            raise IncompatibleSchemaVersionError(f"Unsupported schema_version: '{self.schema_version}'", detected_version=self.schema_version)

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
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> AuditAlert:
        """Deserialize alert from dictionary with schema version migration."""
        migrated_data = migrate_alert_payload(data)
        cat_raw = migrated_data.get("category", AuditDiagnosticCategory.GENERAL_DIAGNOSTIC.value)
        try:
            category = AuditDiagnosticCategory(cat_raw)
        except ValueError:
            category = AuditDiagnosticCategory.GENERAL_DIAGNOSTIC

        sev_raw = migrated_data.get("severity", AuditDiagnosticSeverity.WARNING.value)
        try:
            severity = AuditDiagnosticSeverity(sev_raw)
        except ValueError:
            severity = AuditDiagnosticSeverity.WARNING

        stat_raw = migrated_data.get("status", AuditAlertStatus.TRIGGERED.value)
        try:
            status = AuditAlertStatus(stat_raw)
        except ValueError:
            status = AuditAlertStatus.TRIGGERED

        ts = float(migrated_data.get("timestamp", 0.0))
        return cls(
            alert_id=str(migrated_data["alert_id"]),
            diagnostic_id=str(migrated_data["diagnostic_id"]),
            correlation_id=str(migrated_data["correlation_id"]),
            investigation_id=str(migrated_data["investigation_id"]),
            timestamp=ts,
            timestamp_iso=str(migrated_data.get("timestamp_iso", format_utc_iso(ts))),
            category=category,
            severity=severity,
            reason=str(migrated_data.get("reason", "")),
            source_references=tuple(str(s) for s in migrated_data.get("source_references", ())),
            status=status,
            cooldown_until=float(migrated_data.get("cooldown_until", 0.0)),
            suppressed_occurrences=int(migrated_data.get("suppressed_occurrences", 0)),
            report_id=migrated_data.get("report_id"),
            evaluation_id=migrated_data.get("evaluation_id"),
            schema_version=str(migrated_data.get("schema_version", CURRENT_DIAGNOSTICS_SCHEMA_VERSION)),
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


# ==============================================================================
# Phase 2, Step 40: Export & Reconciliation Models
# ==============================================================================

class DiagnosticsReconciliationStatus(str, Enum):
    """Integrity and reconciliation status of exported diagnostics against source state."""

    MATCHED = "MATCHED"
    MISMATCHED = "MISMATCHED"
    INCOMPLETE = "INCOMPLETE"
    UNAVAILABLE = "UNAVAILABLE"
    UNVERIFIABLE = "UNVERIFIABLE"


def compute_export_canonical_dict(
    schema_version: str,
    investigation_id: Optional[str],
    diagnostics: Sequence[AuditDiagnosticRecord],
    alerts: Sequence[AuditAlert],
    source_references: Sequence[str],
    retention_status: str,
    is_truncated: bool,
) -> Dict[str, Any]:
    """Build a deterministically sorted canonical dictionary for fingerprint generation."""
    sorted_diags = sorted(
        [d.to_dict() for d in diagnostics],
        key=lambda d: (d.get("timestamp", 0.0), d.get("diagnostic_id", "")),
    )
    sorted_alerts = sorted(
        [a.to_dict() for a in alerts],
        key=lambda a: (a.get("timestamp", 0.0), a.get("alert_id", "")),
    )
    sorted_refs = sorted(set(str(r) for r in source_references))
    return {
        "schema_version": str(schema_version),
        "investigation_id": str(investigation_id) if investigation_id else None,
        "diagnostics": sorted_diags,
        "alerts": sorted_alerts,
        "record_counts": {
            "diagnostics": len(sorted_diags),
            "alerts": len(sorted_alerts),
        },
        "source_references": sorted_refs,
        "retention_status": str(retention_status),
        "is_truncated": bool(is_truncated),
    }


def compute_export_content_fingerprint(canonical_dict: Dict[str, Any]) -> str:
    """Generate deterministic SHA-256 fingerprint for canonical export representation."""
    canonical_json = json.dumps(canonical_dict, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def compute_deterministic_export_id(created_at: float, content_fingerprint: str) -> str:
    """Generate a deterministic export ID based on creation time and content fingerprint."""
    safe_ts = 0.0 if (created_at is None or math.isnan(created_at) or math.isinf(created_at)) else created_at
    h = hashlib.sha256(f"{safe_ts}:{content_fingerprint}".encode("utf-8")).hexdigest()[:16]
    return f"export_{h}"


def compute_deterministic_reconciliation_id(export_id: str, timestamp: float) -> str:
    """Generate a deterministic reconciliation report ID."""
    safe_ts = 0.0 if (timestamp is None or math.isnan(timestamp) or math.isinf(timestamp)) else timestamp
    h = hashlib.sha256(f"{export_id}:{safe_ts}".encode("utf-8")).hexdigest()[:16]
    return f"recon_{h}"


@dataclass(frozen=True)
class DiagnosticsExportRequest:
    """Immutable parameter specification for diagnostic and alert export."""

    investigation_id: Optional[str] = None
    category: Optional[AuditDiagnosticCategory] = None
    severity: Optional[AuditDiagnosticSeverity] = None
    include_alerts: bool = True
    include_suppressed: bool = False
    active_only: bool = False
    include_audit_references: bool = True
    include_reliability_references: bool = True
    limit: int = 100
    schema_version: str = CURRENT_DIAGNOSTICS_SCHEMA_VERSION
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.investigation_id:
            validate_safe_id(self.investigation_id, "investigation_id")
        status, _ = detect_and_validate_schema_version(self.schema_version)
        if status == SchemaVersionCompatibilityStatus.MALFORMED:
            raise MalformedSchemaVersionError(f"Malformed schema_version: '{self.schema_version}'", detected_version=self.schema_version)
        if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
            raise IncompatibleSchemaVersionError(f"Unsupported schema_version: '{self.schema_version}'", detected_version=self.schema_version)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize export request to dictionary."""
        return {
            "investigation_id": self.investigation_id,
            "category": self.category.value if self.category else None,
            "severity": self.severity.value if self.severity else None,
            "include_alerts": self.include_alerts,
            "include_suppressed": self.include_suppressed,
            "active_only": self.active_only,
            "include_audit_references": self.include_audit_references,
            "include_reliability_references": self.include_reliability_references,
            "limit": self.limit,
            "schema_version": self.schema_version,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DiagnosticsExportRequest:
        """Deserialize export request from dictionary."""
        raw_ver = data.get("schema_version")
        status, ver = detect_and_validate_schema_version(raw_ver)
        if status == SchemaVersionCompatibilityStatus.MALFORMED:
            raise MalformedSchemaVersionError(f"Malformed schema_version: '{raw_ver}'")
        if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
            raise IncompatibleSchemaVersionError(f"Unsupported schema_version: '{ver}'")

        final_ver = (
            CURRENT_DIAGNOSTICS_SCHEMA_VERSION
            if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER and ver == LEGACY_UNVERSIONED
            else str(data.get("schema_version", CURRENT_DIAGNOSTICS_SCHEMA_VERSION))
        )

        cat_val = None
        if data.get("category"):
            try:
                cat_val = AuditDiagnosticCategory(str(data["category"]))
            except ValueError:
                pass

        sev_val = None
        if data.get("severity"):
            try:
                sev_val = AuditDiagnosticSeverity(str(data["severity"]))
            except ValueError:
                pass

        limit_val = int(data.get("limit") or data.get("max_records") or 100)

        return cls(
            investigation_id=data.get("investigation_id"),
            category=cat_val,
            severity=sev_val,
            include_alerts=bool(data.get("include_alerts", True)),
            include_suppressed=bool(data.get("include_suppressed", False)),
            active_only=bool(data.get("active_only", False)),
            include_audit_references=bool(data.get("include_audit_references", True)),
            include_reliability_references=bool(data.get("include_reliability_references", True)),
            limit=limit_val,
            schema_version=final_ver,
            metadata=dict(data.get("metadata", {})),
        )


@dataclass(frozen=True)
class DiagnosticsExportResult:
    """Immutable, deterministic export snapshot of diagnostics, alerts, and retention metadata."""

    export_id: str
    request: DiagnosticsExportRequest
    created_at: float
    created_at_iso: str
    schema_version: str
    diagnostics: Tuple[AuditDiagnosticRecord, ...]
    alerts: Tuple[AuditAlert, ...]
    record_counts: Dict[str, int]
    source_references: Tuple[str, ...]
    retention_status: str
    is_truncated: bool
    content_fingerprint: str
    omitted_reasons: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_safe_id(self.export_id, "export_id")
        status, _ = detect_and_validate_schema_version(self.schema_version)
        if status == SchemaVersionCompatibilityStatus.MALFORMED:
            raise MalformedSchemaVersionError(f"Malformed schema_version: '{self.schema_version}'", detected_version=self.schema_version)
        if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
            raise IncompatibleSchemaVersionError(f"Unsupported schema_version: '{self.schema_version}'", detected_version=self.schema_version)

    def to_canonical_json(self) -> str:
        """Produce deterministic canonical JSON string."""
        can_dict = compute_export_canonical_dict(
            schema_version=self.schema_version,
            investigation_id=self.request.investigation_id,
            diagnostics=self.diagnostics,
            alerts=self.alerts,
            source_references=self.source_references,
            retention_status=self.retention_status,
            is_truncated=self.is_truncated,
        )
        return json.dumps(can_dict, sort_keys=True, separators=(",", ":"))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize export result to dictionary."""
        return {
            "export_id": self.export_id,
            "request": self.request.to_dict(),
            "created_at": self.created_at,
            "created_at_iso": self.created_at_iso,
            "schema_version": self.schema_version,
            "diagnostics": [d.to_dict() for d in self.diagnostics],
            "alerts": [a.to_dict() for a in self.alerts],
            "record_counts": dict(self.record_counts),
            "source_references": list(self.source_references),
            "retention_status": self.retention_status,
            "is_truncated": self.is_truncated,
            "content_fingerprint": self.content_fingerprint,
            "omitted_reasons": dict(self.omitted_reasons),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DiagnosticsExportResult:
        """Deserialize export result from dictionary with schema version migration."""
        migrated_data = migrate_export_payload(data)
        req = DiagnosticsExportRequest.from_dict(migrated_data.get("request", {}))
        diags = tuple(AuditDiagnosticRecord.from_dict(d) for d in migrated_data.get("diagnostics", []))
        alerts = tuple(AuditAlert.from_dict(a) for a in migrated_data.get("alerts", []))
        ts = float(migrated_data.get("created_at", 0.0))
        return cls(
            export_id=str(migrated_data["export_id"]),
            request=req,
            created_at=ts,
            created_at_iso=str(migrated_data.get("created_at_iso", format_utc_iso(ts))),
            schema_version=str(migrated_data.get("schema_version", CURRENT_DIAGNOSTICS_SCHEMA_VERSION)),
            diagnostics=diags,
            alerts=alerts,
            record_counts=dict(migrated_data.get("record_counts", {})),
            source_references=tuple(str(s) for s in migrated_data.get("source_references", ())),
            retention_status=str(migrated_data.get("retention_status", "COMPLETE")),
            is_truncated=bool(migrated_data.get("is_truncated", False)),
            content_fingerprint=str(migrated_data.get("content_fingerprint", "")),
            omitted_reasons=dict(migrated_data.get("omitted_reasons", {})),
        )


@dataclass(frozen=True)
class DiagnosticsReconciliationDiscrepancy:
    """Individual discrepancy identified during export reconciliation against source evidence."""

    discrepancy_type: str
    record_id: Optional[str] = None
    investigation_id: Optional[str] = None
    description: str = ""
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    is_retention_limitation: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Serialize discrepancy to dictionary."""
        return {
            "discrepancy_type": self.discrepancy_type,
            "record_id": self.record_id,
            "investigation_id": self.investigation_id,
            "description": self.description,
            "expected": self.expected,
            "actual": self.actual,
            "is_retention_limitation": self.is_retention_limitation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DiagnosticsReconciliationDiscrepancy:
        """Deserialize discrepancy from dictionary."""
        return cls(
            discrepancy_type=str(data.get("discrepancy_type", "UNKNOWN")),
            record_id=data.get("record_id"),
            investigation_id=data.get("investigation_id"),
            description=str(data.get("description", "")),
            expected=data.get("expected"),
            actual=data.get("actual"),
            is_retention_limitation=bool(data.get("is_retention_limitation", False)),
        )


@dataclass(frozen=True)
class DiagnosticsReconciliationReport:
    """Immutable reconciliation report comparing exported diagnostic records with active source state."""

    reconciliation_id: str
    export_id: str
    timestamp: float
    timestamp_iso: str
    status: DiagnosticsReconciliationStatus
    total_exported_diagnostics: int
    total_exported_alerts: int
    matched_diagnostics: int
    matched_alerts: int
    discrepancies: Tuple[DiagnosticsReconciliationDiscrepancy, ...]
    source_retention_status: str
    is_source_truncated: bool
    fingerprint_verified: bool
    summary_notes: str = ""
    schema_version: str = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        validate_safe_id(self.reconciliation_id, "reconciliation_id")
        validate_safe_id(self.export_id, "export_id")
        status, _ = detect_and_validate_schema_version(self.schema_version)
        if status == SchemaVersionCompatibilityStatus.MALFORMED:
            raise MalformedSchemaVersionError(f"Malformed schema_version: '{self.schema_version}'", detected_version=self.schema_version)
        if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
            raise IncompatibleSchemaVersionError(f"Unsupported schema_version: '{self.schema_version}'", detected_version=self.schema_version)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize reconciliation report to dictionary."""
        return {
            "reconciliation_id": self.reconciliation_id,
            "export_id": self.export_id,
            "timestamp": self.timestamp,
            "timestamp_iso": self.timestamp_iso,
            "status": self.status.value,
            "total_exported_diagnostics": self.total_exported_diagnostics,
            "total_exported_alerts": self.total_exported_alerts,
            "matched_diagnostics": self.matched_diagnostics,
            "matched_alerts": self.matched_alerts,
            "discrepancies": [d.to_dict() for d in self.discrepancies],
            "source_retention_status": self.source_retention_status,
            "is_source_truncated": self.is_source_truncated,
            "fingerprint_verified": self.fingerprint_verified,
            "summary_notes": self.summary_notes,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DiagnosticsReconciliationReport:
        """Deserialize reconciliation report from dictionary with schema version migration."""
        migrated_data = migrate_reconciliation_payload(data)
        stat_raw = migrated_data.get("status", DiagnosticsReconciliationStatus.UNVERIFIABLE.value)
        try:
            status = DiagnosticsReconciliationStatus(stat_raw)
        except ValueError:
            status = DiagnosticsReconciliationStatus.UNVERIFIABLE

        discrepancies = tuple(DiagnosticsReconciliationDiscrepancy.from_dict(d) for d in migrated_data.get("discrepancies", []))
        ts = float(migrated_data.get("timestamp", 0.0))
        return cls(
            reconciliation_id=str(migrated_data["reconciliation_id"]),
            export_id=str(migrated_data["export_id"]),
            timestamp=ts,
            timestamp_iso=str(migrated_data.get("timestamp_iso", format_utc_iso(ts))),
            status=status,
            total_exported_diagnostics=int(migrated_data.get("total_exported_diagnostics", 0)),
            total_exported_alerts=int(migrated_data.get("total_exported_alerts", 0)),
            matched_diagnostics=int(migrated_data.get("matched_diagnostics", 0)),
            matched_alerts=int(migrated_data.get("matched_alerts", 0)),
            discrepancies=discrepancies,
            source_retention_status=str(migrated_data.get("source_retention_status", "COMPLETE")),
            is_source_truncated=bool(migrated_data.get("is_source_truncated", False)),
            fingerprint_verified=bool(migrated_data.get("fingerprint_verified", False)),
            summary_notes=str(migrated_data.get("summary_notes", "")),
            schema_version=str(migrated_data.get("schema_version", CURRENT_DIAGNOSTICS_SCHEMA_VERSION)),
        )
