"""KAISER — Historical Review Audit Diagnostics Persistence Schema Versioning & Migration.

Phase 2, Step 43.
Centralized schema version detection, validation, migration adapters, and compatibility
contract for operational diagnostics, alerts, exports, reconciliations, and retention state.
"""

from __future__ import annotations

import copy
import re
from enum import Enum
from typing import Any, Dict, Mapping, Optional, Tuple

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

