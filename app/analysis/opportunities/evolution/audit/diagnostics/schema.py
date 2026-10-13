"""KAISER — Historical Review Audit Diagnostics Persistence Schema Versioning & Migration.

Phase 2, Step 43.
Centralized schema version detection, validation, migration adapters, and compatibility
contract for operational diagnostics, alerts, exports, reconciliations, and retention state.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from app.core.exceptions import ValidationError

# Current diagnostics persistence schema version
CURRENT_DIAGNOSTICS_SCHEMA_VERSION = "1.0.0"

# Set of all supported schema versions (including current and older supported versions)
SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS = frozenset({"0.9.0", "1.0.0"})

# Identifier for supported legacy data without an explicit schema version
LEGACY_UNVERSIONED = "legacy_unversioned"

# SemVer regex pattern: major.minor.patch with optional pre-release / build metadata
SEMVER_REGEX = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[\da-zA-Z-]+(?:\.[\da-zA-Z-]+)*)?(?:\+[\da-zA-Z-]+)?$"
)


class SchemaVersionCompatibilityStatus(str, Enum):
    """Compatibility classification for a diagnostic payload schema version."""

    CURRENT = "CURRENT"
    SUPPORTED_OLDER = "SUPPORTED_OLDER"
    UNSUPPORTED_FUTURE = "UNSUPPORTED_FUTURE"
    MALFORMED = "MALFORMED"


class DiagnosticsRecordType(str, Enum):
    """Supported persistence record types within the diagnostics subsystem."""

    DIAGNOSTIC = "DIAGNOSTIC"
    ALERT = "ALERT"
    EXPORT = "EXPORT"
    RECONCILIATION = "RECONCILIATION"
    RETENTION_STATS = "RETENTION_STATS"


class SchemaCompatibilityOutcome(str, Enum):
    """Matrix outcome classifications for schema versions and payload states."""

    DIRECT_COMPATIBLE = "DIRECT_COMPATIBLE"
    MIGRATED_COMPATIBLE = "MIGRATED_COMPATIBLE"
    UNVERIFIABLE_SOURCE = "UNVERIFIABLE_SOURCE"
    UNSUPPORTED_FUTURE = "UNSUPPORTED_FUTURE"
    MALFORMED = "MALFORMED"
    CORRUPT_PAYLOAD = "CORRUPT_PAYLOAD"


@dataclass(frozen=True)
class CompatibilityMatrixEntry:
    """Documented, test-backed compatibility matrix entry for a record type and version."""

    record_type: DiagnosticsRecordType
    version_category: str
    expected_outcome: SchemaCompatibilityOutcome
    target_version: Optional[str]
    adapter_function: Optional[str]
    exception_class: Optional[str]
    description: str
    non_destructive_guarantee: str
    test_scenario_ref: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_type": self.record_type.value,
            "version_category": self.version_category,
            "expected_outcome": self.expected_outcome.value,
            "target_version": self.target_version,
            "adapter_function": self.adapter_function,
            "exception_class": self.exception_class,
            "description": self.description,
            "non_destructive_guarantee": self.non_destructive_guarantee,
            "test_scenario_ref": self.test_scenario_ref,
        }


@dataclass(frozen=True)
class MigrationAuditRecord:
    """Immutable audit record representing a single migration or hydration outcome."""

    record_type: str
    record_id: Optional[str]
    source_version: Optional[str]
    target_version: str
    outcome: str
    timestamp: float
    timestamp_iso: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_type": self.record_type,
            "record_id": self.record_id,
            "source_version": self.source_version,
            "target_version": self.target_version,
            "outcome": self.outcome,
            "timestamp": self.timestamp,
            "timestamp_iso": self.timestamp_iso,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MigrationAuditRecord:
        """Construct MigrationAuditRecord from a mapping representation."""
        if not isinstance(data, Mapping):
            raise ValidationError(f"Expected mapping for MigrationAuditRecord, got {type(data).__name__}")
        return cls(
            record_type=str(data.get("record_type", "UNKNOWN")),
            record_id=str(data.get("record_id")) if data.get("record_id") is not None else None,
            source_version=str(data.get("source_version")) if data.get("source_version") is not None else None,
            target_version=str(data.get("target_version", CURRENT_DIAGNOSTICS_SCHEMA_VERSION)),
            outcome=str(data.get("outcome", "UNKNOWN")),
            timestamp=float(data.get("timestamp", 0.0)),
            timestamp_iso=str(data.get("timestamp_iso", "")),
            details=dict(data.get("details", {})),
        )


@dataclass(frozen=True)
class MigrationAuditSummary:
    """Aggregate summary of migration audit outcomes across diagnostics persistence."""

    total_inspected: int
    successful_migrations: int
    direct_compatible_loads: int
    unsupported_future_skipped: int
    malformed_records_skipped: int
    migration_failures: int
    last_migration_iso: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_inspected": self.total_inspected,
            "successful_migrations": self.successful_migrations,
            "direct_compatible_loads": self.direct_compatible_loads,
            "unsupported_future_skipped": self.unsupported_future_skipped,
            "malformed_records_skipped": self.malformed_records_skipped,
            "migration_failures": self.migration_failures,
            "last_migration_iso": self.last_migration_iso,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MigrationAuditSummary:
        """Construct MigrationAuditSummary from a mapping representation."""
        if not isinstance(data, Mapping):
            raise ValidationError(f"Expected mapping for MigrationAuditSummary, got {type(data).__name__}")
        return cls(
            total_inspected=int(data.get("total_inspected", data.get("cumulative_total_inspected", 0))),
            successful_migrations=int(data.get("successful_migrations", data.get("cumulative_successful_migrations", 0))),
            direct_compatible_loads=int(data.get("direct_compatible_loads", data.get("cumulative_direct_compatible_loads", 0))),
            unsupported_future_skipped=int(data.get("unsupported_future_skipped", data.get("cumulative_unsupported_future_skipped", 0))),
            malformed_records_skipped=int(data.get("malformed_records_skipped", data.get("cumulative_malformed_records_skipped", 0))),
            migration_failures=int(data.get("migration_failures", data.get("cumulative_migration_failures", 0))),
            last_migration_iso=str(data.get("last_migration_iso")) if data.get("last_migration_iso") else None,
        )


class SchemaVersionError(ValidationError):
    """Base exception for diagnostic schema versioning and compatibility errors."""

    def __init__(self, message: str, detected_version: Any = None) -> None:
        super().__init__(message)
        self.detected_version = detected_version


class IncompatibleSchemaVersionError(SchemaVersionError):
    """Raised when an unsupported future or incompatible schema version is encountered."""

    pass


class MalformedSchemaVersionError(SchemaVersionError):
    """Raised when a schema version identifier is invalid, non-string, or malformed."""

    pass


def _parse_semver_tuple(version_str: str) -> Optional[Tuple[int, int, int]]:
    """Parse major, minor, patch integers from valid semver string."""
    m = SEMVER_REGEX.match(version_str)
    if not m:
        return None
    try:
        return int(m.group(1)), int(m.group(2)), int(m.group(3))
    except (ValueError, TypeError):
        return None


def detect_and_validate_schema_version(
    raw_version: Any,
) -> Tuple[SchemaVersionCompatibilityStatus, Optional[str]]:
    """Centralized detection and validation of schema version strings.

    Args:
        raw_version: Raw version attribute from dictionary or payload.

    Returns:
        Tuple of (SchemaVersionCompatibilityStatus, normalized_version_string_or_None).
    """
    # 1. Missing or None represents supported legacy unversioned data
    if raw_version is None:
        return SchemaVersionCompatibilityStatus.SUPPORTED_OLDER, LEGACY_UNVERSIONED

    # 2. Non-string types are strictly malformed
    if not isinstance(raw_version, str):
        return SchemaVersionCompatibilityStatus.MALFORMED, None

    cleaned = raw_version.strip()
    if not cleaned:
        return SchemaVersionCompatibilityStatus.MALFORMED, None

    # 3. Check semver syntax
    v_tuple = _parse_semver_tuple(cleaned)
    if v_tuple is None:
        return SchemaVersionCompatibilityStatus.MALFORMED, None

    # 4. Current schema version
    if cleaned == CURRENT_DIAGNOSTICS_SCHEMA_VERSION:
        return SchemaVersionCompatibilityStatus.CURRENT, CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # 5. Supported older schema version
    if cleaned in SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS:
        return SchemaVersionCompatibilityStatus.SUPPORTED_OLDER, cleaned

    # 6. Compare version tuple against current version (1, 0, 0)
    current_tuple = _parse_semver_tuple(CURRENT_DIAGNOSTICS_SCHEMA_VERSION) or (1, 0, 0)
    if v_tuple > current_tuple:
        return SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE, cleaned

    # 7. Older version not in supported set
    return SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE, cleaned


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


def sanitize_payload_text(text: Any, max_length: int = 512) -> str:
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


def sanitize_audit_details(val: Any, max_length: int = 512) -> Any:
    """Recursively sanitize mapping, sequence, or primitive values for migration audit records."""
    if isinstance(val, str):
        return sanitize_payload_text(val, max_length=max_length)
    if isinstance(val, Mapping):
        return {str(k): sanitize_audit_details(v, max_length=max_length) for k, v in val.items()}
    if isinstance(val, (list, tuple, set)):
        return [sanitize_audit_details(x, max_length=max_length) for x in val]
    if isinstance(val, (int, float, bool)) or val is None:
        return val
    return sanitize_payload_text(str(val), max_length=max_length)


# ==============================================================================
# Migration Adapters
# ==============================================================================

def migrate_diagnostic_payload(
    data: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate and migrate a diagnostic record payload to the current schema.

    Preserves original data without mutation. Upgrades legacy unversioned and
    supported older version ('0.9.0') representations to current version '1.0.0'.

    Raises:
        ValidationError: If payload is not a valid mapping.
        MalformedSchemaVersionError: If schema_version is invalid or malformed.
        IncompatibleSchemaVersionError: If schema_version is an unsupported future version.
    """
    if not isinstance(data, Mapping):
        raise ValidationError(f"Expected mapping payload for diagnostic record, got {type(data).__name__}")

    raw_ver = data.get("schema_version")
    status, ver = detect_and_validate_schema_version(raw_ver)

    if status == SchemaVersionCompatibilityStatus.MALFORMED:
        raise MalformedSchemaVersionError(
            f"Malformed diagnostic record schema version: '{raw_ver}'",
            detected_version=raw_ver,
        )
    if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
        raise IncompatibleSchemaVersionError(
            f"Unsupported future diagnostic record schema version '{ver}'. "
            f"Current supported version: '{CURRENT_DIAGNOSTICS_SCHEMA_VERSION}'.",
            detected_version=ver,
        )

    result = copy.deepcopy(dict(data))

    if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER:
        if ver == LEGACY_UNVERSIONED:
            # Upgrade legacy unversioned record to 1.0.0
            result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION
            # Ensure safe optional defaults are present
            if "source_references" not in result or result["source_references"] is None:
                result["source_references"] = []
            if "details" not in result or result["details"] is None:
                result["details"] = {}
            if "sanitized_summary" not in result or result["sanitized_summary"] is None:
                if "summary" in result:
                    result["sanitized_summary"] = result.pop("summary")
                else:
                    result["sanitized_summary"] = ""
            if "error_category" not in result:
                result["error_category"] = None
        elif ver == "0.9.0":
            # Map older 0.9.0 aliases to current 1.0.0 representation
            if "diagnostic_category" in result and "category" not in result:
                result["category"] = result.pop("diagnostic_category")
            if "summary" in result and "sanitized_summary" not in result:
                result["sanitized_summary"] = result.pop("summary")
            if "error" in result and "error_category" not in result:
                result["error_category"] = result.pop("error")
            result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    elif status == SchemaVersionCompatibilityStatus.CURRENT:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    # Ensure safe correlation_id exists for legacy payloads
    if "correlation_id" not in result or not result["correlation_id"]:
        d_id = str(result.get("diagnostic_id", "diag"))
        result["correlation_id"] = f"corr_{d_id[-12:]}"

    # Apply secret sanitization across messages and summaries
    if "message" in result and isinstance(result["message"], str):
        result["message"] = sanitize_payload_text(result["message"])
    if "sanitized_summary" in result and isinstance(result["sanitized_summary"], str):
        result["sanitized_summary"] = sanitize_payload_text(result["sanitized_summary"])

    return result


def migrate_alert_payload(
    data: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate and migrate an alert record payload to the current schema.

    Preserves original data without mutation.

    Raises:
        ValidationError: If payload is not a valid mapping.
        MalformedSchemaVersionError: If schema_version is invalid or malformed.
        IncompatibleSchemaVersionError: If schema_version is an unsupported future version.
    """
    if not isinstance(data, Mapping):
        raise ValidationError(f"Expected mapping payload for alert record, got {type(data).__name__}")

    raw_ver = data.get("schema_version")
    status, ver = detect_and_validate_schema_version(raw_ver)

    if status == SchemaVersionCompatibilityStatus.MALFORMED:
        raise MalformedSchemaVersionError(
            f"Malformed alert record schema version: '{raw_ver}'",
            detected_version=raw_ver,
        )
    if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
        raise IncompatibleSchemaVersionError(
            f"Unsupported future alert record schema version '{ver}'. "
            f"Current supported version: '{CURRENT_DIAGNOSTICS_SCHEMA_VERSION}'.",
            detected_version=ver,
        )

    result = copy.deepcopy(dict(data))

    if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER:
        if ver == LEGACY_UNVERSIONED:
            result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION
            if "source_references" not in result or result["source_references"] is None:
                result["source_references"] = []
            if "status" not in result or result["status"] is None:
                result["status"] = "TRIGGERED"
            if "cooldown_until" not in result:
                result["cooldown_until"] = 0.0
            if "suppressed_occurrences" not in result:
                result["suppressed_occurrences"] = 0
        elif ver == "0.9.0":
            if "alert_reason" in result and "reason" not in result:
                result["reason"] = result.pop("alert_reason")
            if "state" in result and "status" not in result:
                result["status"] = result.pop("state")
            if "active" in result and "status" not in result:
                result["status"] = "ACTIVE" if result.pop("active") else "RESOLVED"
            result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    elif status == SchemaVersionCompatibilityStatus.CURRENT:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    if "reason" in result and isinstance(result["reason"], str):
        result["reason"] = sanitize_payload_text(result["reason"])

    return result


def migrate_export_payload(
    data: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate and migrate an export snapshot payload to the current schema.

    Preserves original data without mutation. Recursively migrates nested
    diagnostic and alert records.

    Raises:
        ValidationError: If payload is not a valid mapping.
        MalformedSchemaVersionError: If schema_version is invalid or malformed.
        IncompatibleSchemaVersionError: If schema_version is an unsupported future version.
    """
    if not isinstance(data, Mapping):
        raise ValidationError(f"Expected mapping payload for export result, got {type(data).__name__}")

    raw_ver = data.get("schema_version")
    status, ver = detect_and_validate_schema_version(raw_ver)

    if status == SchemaVersionCompatibilityStatus.MALFORMED:
        raise MalformedSchemaVersionError(
            f"Malformed export snapshot schema version: '{raw_ver}'",
            detected_version=raw_ver,
        )
    if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
        raise IncompatibleSchemaVersionError(
            f"Unsupported future export snapshot schema version '{ver}'. "
            f"Current supported version: '{CURRENT_DIAGNOSTICS_SCHEMA_VERSION}'.",
            detected_version=ver,
        )

    result = copy.deepcopy(dict(data))

    if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

        # Normalize request if present
        req = result.get("request")
        if isinstance(req, dict):
            req_copy = dict(req)
            if "max_records" in req_copy and "limit" not in req_copy:
                req_copy["limit"] = req_copy.pop("max_records")
            req_copy["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION
            result["request"] = req_copy

        # Migrate nested diagnostics
        if "diagnostics" in result and isinstance(result["diagnostics"], list):
            migrated_diags = []
            for d in result["diagnostics"]:
                if isinstance(d, dict):
                    mig_d = migrate_diagnostic_payload(d)
                    migrated_diags.append(mig_d)
                else:
                    migrated_diags.append(d)
            result["diagnostics"] = migrated_diags

        # Migrate nested alerts
        if "alerts" in result and isinstance(result["alerts"], list):
            migrated_alerts = []
            for a in result["alerts"]:
                if isinstance(a, dict):
                    mig_a = migrate_alert_payload(a)
                    migrated_alerts.append(mig_a)
                else:
                    migrated_alerts.append(a)
            result["alerts"] = migrated_alerts

    elif status == SchemaVersionCompatibilityStatus.CURRENT:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    return result


def migrate_reconciliation_payload(
    data: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate and migrate a reconciliation report payload to the current schema.

    Preserves original data without mutation.

    Raises:
        ValidationError: If payload is not a valid mapping.
        MalformedSchemaVersionError: If schema_version is invalid or malformed.
        IncompatibleSchemaVersionError: If schema_version is an unsupported future version.
    """
    if not isinstance(data, Mapping):
        raise ValidationError(f"Expected mapping payload for reconciliation report, got {type(data).__name__}")

    raw_ver = data.get("schema_version")
    status, ver = detect_and_validate_schema_version(raw_ver)

    if status == SchemaVersionCompatibilityStatus.MALFORMED:
        raise MalformedSchemaVersionError(
            f"Malformed reconciliation report schema version: '{raw_ver}'",
            detected_version=raw_ver,
        )
    if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
        raise IncompatibleSchemaVersionError(
            f"Unsupported future reconciliation report schema version '{ver}'. "
            f"Current supported version: '{CURRENT_DIAGNOSTICS_SCHEMA_VERSION}'.",
            detected_version=ver,
        )

    result = copy.deepcopy(dict(data))

    if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION
        if ver == "0.9.0":
            if "verdict" in result and "status" not in result:
                result["status"] = result.pop("verdict")
            if "fingerprint_match" in result and "fingerprint_verified" not in result:
                result["fingerprint_verified"] = result.pop("fingerprint_match")
    elif status == SchemaVersionCompatibilityStatus.CURRENT:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    return result


def migrate_retention_stats_payload(
    data: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate and migrate retention statistics dictionary to the current schema.

    Preserves original data without mutation.

    Raises:
        ValidationError: If payload is not a valid mapping.
        MalformedSchemaVersionError: If schema_version is invalid or malformed.
        IncompatibleSchemaVersionError: If schema_version is an unsupported future version.
    """
    if not isinstance(data, Mapping):
        raise ValidationError(f"Expected mapping payload for retention stats, got {type(data).__name__}")

    raw_ver = data.get("schema_version")
    status, ver = detect_and_validate_schema_version(raw_ver)

    if status == SchemaVersionCompatibilityStatus.MALFORMED:
        raise MalformedSchemaVersionError(
            f"Malformed retention stats schema version: '{raw_ver}'",
            detected_version=raw_ver,
        )
    if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
        raise IncompatibleSchemaVersionError(
            f"Unsupported future retention stats schema version '{ver}'. "
            f"Current supported version: '{CURRENT_DIAGNOSTICS_SCHEMA_VERSION}'.",
            detected_version=ver,
        )

    result = copy.deepcopy(dict(data))

    if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION
        if ver == "0.9.0":
            if "evicted_count" in result and "evicted_diagnostics_count" not in result:
                result["evicted_diagnostics_count"] = result.pop("evicted_count")
    elif status == SchemaVersionCompatibilityStatus.CURRENT:
        result["schema_version"] = CURRENT_DIAGNOSTICS_SCHEMA_VERSION

    return result


# ==============================================================================
# Diagnostics Schema Compatibility Matrix Registry
# ==============================================================================

DIAGNOSTICS_COMPATIBILITY_MATRIX: Dict[Tuple[DiagnosticsRecordType, str], CompatibilityMatrixEntry] = {
    # --------------------------------------------------------------------------
    # 1. DIAGNOSTIC RECORDS
    # --------------------------------------------------------------------------
    (DiagnosticsRecordType.DIAGNOSTIC, "legacy_unversioned"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.DIAGNOSTIC,
        version_category="legacy_unversioned",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_diagnostic_payload",
        exception_class=None,
        description="Legacy unversioned diagnostic payload upgraded to 1.0.0 with optional defaults and sanitization.",
        non_destructive_guarantee="Original input payload deepcopied; source dictionary not mutated in place.",
        test_scenario_ref="SCENARIO_1_DIAGNOSTIC_LEGACY",
    ),
    (DiagnosticsRecordType.DIAGNOSTIC, "0.9.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.DIAGNOSTIC,
        version_category="0.9.0",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_diagnostic_payload",
        exception_class=None,
        description="Schema 0.9.0 diagnostic payload migrated: aliases diagnostic_category, summary, error mapped to 1.0.0.",
        non_destructive_guarantee="Original input payload preserved; produces distinct normalized representation.",
        test_scenario_ref="SCENARIO_1_DIAGNOSTIC_0_9_0",
    ),
    (DiagnosticsRecordType.DIAGNOSTIC, "1.0.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.DIAGNOSTIC,
        version_category="1.0.0",
        expected_outcome=SchemaCompatibilityOutcome.DIRECT_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_diagnostic_payload",
        exception_class=None,
        description="Current schema 1.0.0 diagnostic payload loaded directly without schema transformation.",
        non_destructive_guarantee="Direct compatible loading without data alteration.",
        test_scenario_ref="SCENARIO_1_DIAGNOSTIC_1_0_0",
    ),
    (DiagnosticsRecordType.DIAGNOSTIC, "unsupported_future"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.DIAGNOSTIC,
        version_category="unsupported_future",
        expected_outcome=SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE,
        target_version=None,
        adapter_function="migrate_diagnostic_payload",
        exception_class="IncompatibleSchemaVersionError",
        description="Unsupported future version rejected; skipped during hydration to prevent silent downgrade.",
        non_destructive_guarantee="Original record preserved in StateStore; never overwritten with downgraded payload.",
        test_scenario_ref="SCENARIO_1_DIAGNOSTIC_FUTURE",
    ),
    (DiagnosticsRecordType.DIAGNOSTIC, "malformed_version"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.DIAGNOSTIC,
        version_category="malformed_version",
        expected_outcome=SchemaCompatibilityOutcome.MALFORMED,
        target_version=None,
        adapter_function="migrate_diagnostic_payload",
        exception_class="MalformedSchemaVersionError",
        description="Non-string, blank, or invalid semver schema version rejected; skipped during hydration.",
        non_destructive_guarantee="Stored record retained in persistence without corruption or partial write.",
        test_scenario_ref="SCENARIO_1_DIAGNOSTIC_MALFORMED",
    ),
    (DiagnosticsRecordType.DIAGNOSTIC, "corrupt_payload"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.DIAGNOSTIC,
        version_category="corrupt_payload",
        expected_outcome=SchemaCompatibilityOutcome.CORRUPT_PAYLOAD,
        target_version=None,
        adapter_function="migrate_diagnostic_payload",
        exception_class="ValidationError",
        description="Non-mapping payload or missing required identifiers rejected by model validation.",
        non_destructive_guarantee="Quarantined during hydration; valid unrelated records remain unaffected.",
        test_scenario_ref="SCENARIO_1_DIAGNOSTIC_CORRUPT",
    ),

    # --------------------------------------------------------------------------
    # 2. ALERT RECORDS
    # --------------------------------------------------------------------------
    (DiagnosticsRecordType.ALERT, "legacy_unversioned"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.ALERT,
        version_category="legacy_unversioned",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_alert_payload",
        exception_class=None,
        description="Legacy unversioned alert payload upgraded to 1.0.0 with lifecycle defaults.",
        non_destructive_guarantee="Original input payload preserved; deepcopy ensures zero in-place mutation.",
        test_scenario_ref="SCENARIO_2_ALERT_LEGACY",
    ),
    (DiagnosticsRecordType.ALERT, "0.9.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.ALERT,
        version_category="0.9.0",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_alert_payload",
        exception_class=None,
        description="Schema 0.9.0 alert payload migrated: alert_reason mapped to reason, state/active mapped to status.",
        non_destructive_guarantee="Original input dictionary untouched; distinct normalized dictionary returned.",
        test_scenario_ref="SCENARIO_2_ALERT_0_9_0",
    ),
    (DiagnosticsRecordType.ALERT, "1.0.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.ALERT,
        version_category="1.0.0",
        expected_outcome=SchemaCompatibilityOutcome.DIRECT_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_alert_payload",
        exception_class=None,
        description="Current schema 1.0.0 alert payload loaded directly.",
        non_destructive_guarantee="Direct loading without schema alteration.",
        test_scenario_ref="SCENARIO_2_ALERT_1_0_0",
    ),
    (DiagnosticsRecordType.ALERT, "unsupported_future"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.ALERT,
        version_category="unsupported_future",
        expected_outcome=SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE,
        target_version=None,
        adapter_function="migrate_alert_payload",
        exception_class="IncompatibleSchemaVersionError",
        description="Unsupported future alert schema version rejected; skipped during hydration.",
        non_destructive_guarantee="Stored alert payload preserved in StateStore; never partially overwritten.",
        test_scenario_ref="SCENARIO_2_ALERT_FUTURE",
    ),
    (DiagnosticsRecordType.ALERT, "malformed_version"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.ALERT,
        version_category="malformed_version",
        expected_outcome=SchemaCompatibilityOutcome.MALFORMED,
        target_version=None,
        adapter_function="migrate_alert_payload",
        exception_class="MalformedSchemaVersionError",
        description="Malformed or invalid semver alert schema version rejected; skipped during hydration.",
        non_destructive_guarantee="Stored record retained in persistence without corruption.",
        test_scenario_ref="SCENARIO_2_ALERT_MALFORMED",
    ),
    (DiagnosticsRecordType.ALERT, "corrupt_payload"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.ALERT,
        version_category="corrupt_payload",
        expected_outcome=SchemaCompatibilityOutcome.CORRUPT_PAYLOAD,
        target_version=None,
        adapter_function="migrate_alert_payload",
        exception_class="ValidationError",
        description="Non-mapping payload or missing mandatory identifiers rejected by alert validation.",
        non_destructive_guarantee="Quarantined during hydration; other stored alerts remain accessible.",
        test_scenario_ref="SCENARIO_2_ALERT_CORRUPT",
    ),

    # --------------------------------------------------------------------------
    # 3. EXPORT RECORDS
    # --------------------------------------------------------------------------
    (DiagnosticsRecordType.EXPORT, "legacy_unversioned"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.EXPORT,
        version_category="legacy_unversioned",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_export_payload",
        exception_class=None,
        description="Legacy unversioned export snapshot migrated to 1.0.0; nested records recursively migrated.",
        non_destructive_guarantee="Original snapshot mapping not mutated; separate migrated structure generated.",
        test_scenario_ref="SCENARIO_3_EXPORT_LEGACY",
    ),
    (DiagnosticsRecordType.EXPORT, "0.9.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.EXPORT,
        version_category="0.9.0",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_export_payload",
        exception_class=None,
        description="Schema 0.9.0 export snapshot migrated: request.max_records mapped to limit; nested items upgraded.",
        non_destructive_guarantee="Original snapshot dictionary preserved; nested records deepcopied.",
        test_scenario_ref="SCENARIO_3_EXPORT_0_9_0",
    ),
    (DiagnosticsRecordType.EXPORT, "1.0.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.EXPORT,
        version_category="1.0.0",
        expected_outcome=SchemaCompatibilityOutcome.DIRECT_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_export_payload",
        exception_class=None,
        description="Current schema 1.0.0 export snapshot loaded directly with canonical fingerprint match.",
        non_destructive_guarantee="Direct loading without alteration.",
        test_scenario_ref="SCENARIO_3_EXPORT_1_0_0",
    ),
    (DiagnosticsRecordType.EXPORT, "unsupported_future"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.EXPORT,
        version_category="unsupported_future",
        expected_outcome=SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE,
        target_version=None,
        adapter_function="migrate_export_payload",
        exception_class="IncompatibleSchemaVersionError",
        description="Future export snapshot rejected; reconciliation reports UNVERIFIABLE status.",
        non_destructive_guarantee="Stored export preserved in StateStore; reconciliation returns UNVERIFIABLE safely.",
        test_scenario_ref="SCENARIO_3_EXPORT_FUTURE",
    ),
    (DiagnosticsRecordType.EXPORT, "malformed_version"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.EXPORT,
        version_category="malformed_version",
        expected_outcome=SchemaCompatibilityOutcome.MALFORMED,
        target_version=None,
        adapter_function="migrate_export_payload",
        exception_class="MalformedSchemaVersionError",
        description="Malformed export schema version rejected; skipped during hydration; reconciliation reports UNVERIFIABLE.",
        non_destructive_guarantee="Stored export snapshot preserved without corruption.",
        test_scenario_ref="SCENARIO_3_EXPORT_MALFORMED",
    ),
    (DiagnosticsRecordType.EXPORT, "corrupt_payload"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.EXPORT,
        version_category="corrupt_payload",
        expected_outcome=SchemaCompatibilityOutcome.CORRUPT_PAYLOAD,
        target_version=None,
        adapter_function="migrate_export_payload",
        exception_class="ValidationError",
        description="Non-mapping export payload rejected by validation.",
        non_destructive_guarantee="Quarantined safely; existing valid exports remain accessible.",
        test_scenario_ref="SCENARIO_3_EXPORT_CORRUPT",
    ),

    # --------------------------------------------------------------------------
    # 4. RECONCILIATION REPORTS
    # --------------------------------------------------------------------------
    (DiagnosticsRecordType.RECONCILIATION, "legacy_unversioned"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="legacy_unversioned",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_reconciliation_payload",
        exception_class=None,
        description="Legacy reconciliation report migrated to 1.0.0; discrepancies safely loaded.",
        non_destructive_guarantee="Original payload preserved; deepcopy ensures zero in-place modification.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_LEGACY",
    ),
    (DiagnosticsRecordType.RECONCILIATION, "0.9.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="0.9.0",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_reconciliation_payload",
        exception_class=None,
        description="Schema 0.9.0 reconciliation report migrated: verdict mapped to status, fingerprint_match mapped.",
        non_destructive_guarantee="Original input payload preserved intact.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_0_9_0",
    ),
    (DiagnosticsRecordType.RECONCILIATION, "1.0.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="1.0.0",
        expected_outcome=SchemaCompatibilityOutcome.DIRECT_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_reconciliation_payload",
        exception_class=None,
        description="Current schema 1.0.0 reconciliation report loaded directly.",
        non_destructive_guarantee="Direct loading without alteration.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_1_0_0",
    ),
    (DiagnosticsRecordType.RECONCILIATION, "unsupported_future"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="unsupported_future",
        expected_outcome=SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE,
        target_version=None,
        adapter_function="migrate_reconciliation_payload",
        exception_class="IncompatibleSchemaVersionError",
        description="Unsupported future reconciliation report rejected; skipped during hydration.",
        non_destructive_guarantee="Stored reconciliation preserved in StateStore.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_FUTURE",
    ),
    (DiagnosticsRecordType.RECONCILIATION, "malformed_version"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="malformed_version",
        expected_outcome=SchemaCompatibilityOutcome.MALFORMED,
        target_version=None,
        adapter_function="migrate_reconciliation_payload",
        exception_class="MalformedSchemaVersionError",
        description="Malformed reconciliation schema version rejected; skipped during hydration.",
        non_destructive_guarantee="Stored report retained without corruption.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_MALFORMED",
    ),
    (DiagnosticsRecordType.RECONCILIATION, "corrupt_payload"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="corrupt_payload",
        expected_outcome=SchemaCompatibilityOutcome.CORRUPT_PAYLOAD,
        target_version=None,
        adapter_function="migrate_reconciliation_payload",
        exception_class="ValidationError",
        description="Non-mapping reconciliation payload rejected by validation.",
        non_destructive_guarantee="Quarantined safely; other reconciliation reports unaffected.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_CORRUPT",
    ),
    (DiagnosticsRecordType.RECONCILIATION, "unverifiable_source"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RECONCILIATION,
        version_category="unverifiable_source",
        expected_outcome=SchemaCompatibilityOutcome.UNVERIFIABLE_SOURCE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function=None,
        exception_class=None,
        description="Reconciliation when export snapshot is evicted, missing, or incompatible returns UNVERIFIABLE.",
        non_destructive_guarantee="Safe non-crashing outcome exposing explicit verification limitation.",
        test_scenario_ref="SCENARIO_4_RECONCILIATION_UNVERIFIABLE",
    ),

    # --------------------------------------------------------------------------
    # 5. RETENTION STATISTICS
    # --------------------------------------------------------------------------
    (DiagnosticsRecordType.RETENTION_STATS, "legacy_unversioned"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RETENTION_STATS,
        version_category="legacy_unversioned",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_retention_stats_payload",
        exception_class=None,
        description="Legacy retention statistics dictionary migrated to 1.0.0.",
        non_destructive_guarantee="Original dictionary not mutated in place.",
        test_scenario_ref="SCENARIO_5_RETENTION_STATS_LEGACY",
    ),
    (DiagnosticsRecordType.RETENTION_STATS, "0.9.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RETENTION_STATS,
        version_category="0.9.0",
        expected_outcome=SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_retention_stats_payload",
        exception_class=None,
        description="Schema 0.9.0 retention stats migrated: evicted_count mapped to evicted_diagnostics_count.",
        non_destructive_guarantee="Original dictionary preserved intact.",
        test_scenario_ref="SCENARIO_5_RETENTION_STATS_0_9_0",
    ),
    (DiagnosticsRecordType.RETENTION_STATS, "1.0.0"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RETENTION_STATS,
        version_category="1.0.0",
        expected_outcome=SchemaCompatibilityOutcome.DIRECT_COMPATIBLE,
        target_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
        adapter_function="migrate_retention_stats_payload",
        exception_class=None,
        description="Current schema 1.0.0 retention stats loaded directly.",
        non_destructive_guarantee="Direct loading without alteration.",
        test_scenario_ref="SCENARIO_5_RETENTION_STATS_1_0_0",
    ),
    (DiagnosticsRecordType.RETENTION_STATS, "unsupported_future"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RETENTION_STATS,
        version_category="unsupported_future",
        expected_outcome=SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE,
        target_version=None,
        adapter_function="migrate_retention_stats_payload",
        exception_class="IncompatibleSchemaVersionError",
        description="Unsupported future retention stats rejected; fallback to clean initialized counters.",
        non_destructive_guarantee="Stored stats key preserved in StateStore; not corrupted.",
        test_scenario_ref="SCENARIO_5_RETENTION_STATS_FUTURE",
    ),
    (DiagnosticsRecordType.RETENTION_STATS, "malformed_version"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RETENTION_STATS,
        version_category="malformed_version",
        expected_outcome=SchemaCompatibilityOutcome.MALFORMED,
        target_version=None,
        adapter_function="migrate_retention_stats_payload",
        exception_class="MalformedSchemaVersionError",
        description="Malformed retention stats schema version rejected; fallback to clean counters.",
        non_destructive_guarantee="Stored payload retained in StateStore.",
        test_scenario_ref="SCENARIO_5_RETENTION_STATS_MALFORMED",
    ),
    (DiagnosticsRecordType.RETENTION_STATS, "corrupt_payload"): CompatibilityMatrixEntry(
        record_type=DiagnosticsRecordType.RETENTION_STATS,
        version_category="corrupt_payload",
        expected_outcome=SchemaCompatibilityOutcome.CORRUPT_PAYLOAD,
        target_version=None,
        adapter_function="migrate_retention_stats_payload",
        exception_class="ValidationError",
        description="Non-mapping retention stats rejected by validation.",
        non_destructive_guarantee="Fallback safely initialized without affecting StateStore.",
        test_scenario_ref="SCENARIO_5_RETENTION_STATS_CORRUPT",
    ),
}


def query_compatibility_matrix(
    record_type: Union[DiagnosticsRecordType, str],
    version_category: str,
) -> Optional[CompatibilityMatrixEntry]:
    """Retrieve compatibility matrix entry for a record type and version category.

    Args:
        record_type: DiagnosticsRecordType or string (e.g. 'DIAGNOSTIC', 'ALERT').
        version_category: Category string (e.g. '1.0.0', '0.9.0', 'legacy_unversioned',
            'unsupported_future', 'malformed_version', 'corrupt_payload').

    Returns:
        CompatibilityMatrixEntry if found, else None.
    """
    rt_key = record_type.value if isinstance(record_type, DiagnosticsRecordType) else str(record_type).upper()
    try:
        r_type = DiagnosticsRecordType(rt_key)
    except ValueError:
        return None
    return DIAGNOSTICS_COMPATIBILITY_MATRIX.get((r_type, str(version_category).lower()))


def classify_payload_compatibility(
    record_type: Union[DiagnosticsRecordType, str],
    payload: Any,
) -> Tuple[SchemaCompatibilityOutcome, Optional[CompatibilityMatrixEntry]]:
    """Classify a given raw payload according to the compatibility matrix.

    Args:
        record_type: DiagnosticsRecordType or string.
        payload: Raw record payload to evaluate.

    Returns:
        Tuple of (SchemaCompatibilityOutcome, Optional[CompatibilityMatrixEntry]).
    """
    if not isinstance(payload, Mapping):
        entry = query_compatibility_matrix(record_type, "corrupt_payload")
        return SchemaCompatibilityOutcome.CORRUPT_PAYLOAD, entry

    raw_v = payload.get("schema_version")
    status, v_str = detect_and_validate_schema_version(raw_v)

    if status == SchemaVersionCompatibilityStatus.MALFORMED:
        entry = query_compatibility_matrix(record_type, "malformed_version")
        return SchemaCompatibilityOutcome.MALFORMED, entry
    if status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
        entry = query_compatibility_matrix(record_type, "unsupported_future")
        return SchemaCompatibilityOutcome.UNSUPPORTED_FUTURE, entry
    if status == SchemaVersionCompatibilityStatus.SUPPORTED_OLDER:
        cat = "0.9.0" if v_str == "0.9.0" else "legacy_unversioned"
        entry = query_compatibility_matrix(record_type, cat)
        return SchemaCompatibilityOutcome.MIGRATED_COMPATIBLE, entry
    if status == SchemaVersionCompatibilityStatus.CURRENT:
        entry = query_compatibility_matrix(record_type, "1.0.0")
        return SchemaCompatibilityOutcome.DIRECT_COMPATIBLE, entry

    entry = query_compatibility_matrix(record_type, "corrupt_payload")
    return SchemaCompatibilityOutcome.CORRUPT_PAYLOAD, entry


