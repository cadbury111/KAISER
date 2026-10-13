"""KAISER — Historical Review Audit Observability, Alerting & Operational Diagnostics Service.

Provides a thread-safe, bounded, secret-safe diagnostics and informational alerting
service for historical review audit and reliability workflows.

SAFETY & RESEARCH INVARIANTS:
1. Purely informational diagnostics and alerts.
2. In-memory bounded retention; StateStore fallback support.
3. Injected clock support for deterministic testing.
4. Bounded alert cooldown / deduplication to prevent alert storms.
5. Underlying diagnostic records are NEVER suppressed when alerts are deduplicated.
6. Failure and recovery lifecycle tracking.
7. Preserves Settings.execution_locked = True; zero trading or autonomous actions exist.
"""

from __future__ import annotations

import collections
import copy
from datetime import datetime, timezone
from enum import Enum
import hashlib
import logging
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union

from app.analysis.opportunities.evolution.audit.diagnostics.models import (
    AuditAlert,
    AuditAlertStatus,
    AuditDiagnosticCategory,
    AuditDiagnosticRecord,
    AuditDiagnosticSeverity,
    AuditDiagnosticSummary,
    AuditEvidenceNature,
    CREDENTIAL_KEY_REGEX,
    DiagnosticsExportRequest,
    DiagnosticsExportResult,
    DiagnosticsReconciliationDiscrepancy,
    DiagnosticsReconciliationReport,
    DiagnosticsReconciliationStatus,
    SAFE_ID_PATTERN,
    compute_deterministic_alert_id,
    compute_deterministic_diagnostic_id,
    compute_deterministic_export_id,
    compute_deterministic_reconciliation_id,
    compute_export_canonical_dict,
    compute_export_content_fingerprint,
    format_utc_iso,
    sanitize_text,
    validate_safe_id,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore
from app.analysis.opportunities.evolution.audit.diagnostics.schema import (
    CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
    SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS,
    SchemaVersionCompatibilityStatus,
    SchemaVersionError,
    IncompatibleSchemaVersionError,
    MalformedSchemaVersionError,
    detect_and_validate_schema_version,
    migrate_retention_stats_payload,
)

logger = logging.getLogger("kaiser.analysis.evolution.audit.diagnostics")

DEFAULT_MAX_DIAGNOSTICS = 100
DEFAULT_MAX_ALERTS = 100
DEFAULT_MAX_EXPORTS = 50
DEFAULT_MAX_RECONCILIATIONS = 50
DEFAULT_ALERT_COOLDOWN_SECONDS = 60.0

SAFE_REF_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.\:\@]+$")


class HistoricalReviewAuditDiagnosticsService:
    """Operational diagnostics and informational alerting service for historical review audits."""

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        state_store: Optional[StateStore] = None,
        max_diagnostics: int = DEFAULT_MAX_DIAGNOSTICS,
        max_alerts: int = DEFAULT_MAX_ALERTS,
        max_exports: int = DEFAULT_MAX_EXPORTS,
        max_reconciliations: int = DEFAULT_MAX_RECONCILIATIONS,
        alert_cooldown_seconds: float = DEFAULT_ALERT_COOLDOWN_SECONDS,
        time_provider: Optional[Callable[[], float]] = None,
        auto_subscribe: bool = True,
        auto_hydrate: bool = False,
    ) -> None:
        """Initialize the diagnostics, alerting, export, and reconciliation service.

        Args:
            event_bus: Optional EventBus for receiving and publishing events.
            state_store: Optional StateStore for persistent snapshot storage.
            max_diagnostics: Maximum number of diagnostic records to retain.
            max_alerts: Maximum number of alert records to retain.
            max_exports: Maximum number of diagnostic export snapshots to retain.
            max_reconciliations: Maximum number of reconciliation reports to retain.
            alert_cooldown_seconds: Minimum seconds before re-triggering an identical alert.
            time_provider: Injectable timestamp function (defaults to time.time).
            auto_subscribe: If True and event_bus provided, automatically subscribes to audit events.
            auto_hydrate: If True and state_store provided, automatically hydrates in-memory history.
        """
        self._event_bus = event_bus
        self._state_store = state_store
        self._max_diagnostics = max(10, min(1000, max_diagnostics))
        self._max_alerts = max(10, min(1000, max_alerts))
        self._max_exports = max(5, min(500, max_exports))
        self._max_reconciliations = max(5, min(500, max_reconciliations))
        self._alert_cooldown_seconds = max(0.0, alert_cooldown_seconds)
        self._time_provider = time_provider or time.time
        self._lock = threading.RLock()

        # In-memory storage structures
        self._diagnostics: collections.deque[AuditDiagnosticRecord] = collections.deque(
            maxlen=self._max_diagnostics
        )
        self._diagnostics_by_id: Dict[str, AuditDiagnosticRecord] = {}

        self._alerts: collections.deque[AuditAlert] = collections.deque(maxlen=self._max_alerts)
        self._suppressed_alerts: collections.deque[AuditAlert] = collections.deque(maxlen=self._max_alerts)
        self._alerts_by_id: Dict[str, AuditAlert] = {}

        # Export and reconciliation storage
        self._exports: collections.deque[DiagnosticsExportResult] = collections.deque(
            maxlen=self._max_exports
        )
        self._exports_by_id: Dict[str, DiagnosticsExportResult] = {}
        self._evicted_export_ids: collections.deque[str] = collections.deque(maxlen=self._max_exports)

        self._reconciliations: collections.deque[DiagnosticsReconciliationReport] = collections.deque(
            maxlen=self._max_reconciliations
        )
        self._reconciliations_by_id: Dict[str, DiagnosticsReconciliationReport] = {}
        self._evicted_reconciliation_ids: collections.deque[str] = collections.deque(
            maxlen=self._max_reconciliations
        )

        # Alert deduplication and cooldown tracking: (investigation_id, category, severity) -> (last_timestamp, last_alert_id)
        self._last_alert_time: Dict[Tuple[str, str, str], Tuple[float, str]] = {}
        self._suppressed_counts: Dict[Tuple[str, str, str], int] = {}

        # Failure lifecycle tracking: investigation_id -> Set of active failure categories
        self._active_failures: Dict[str, Set[AuditDiagnosticCategory]] = {}

        # Retention and eviction tracking
        self._evicted_diagnostics_count: int = 0
        self._evicted_alerts_count: int = 0
        self._evicted_exports_count: int = 0
        self._evicted_reconciliations_count: int = 0
        self._evicted_diagnostics_by_inv: collections.defaultdict[str, int] = collections.defaultdict(int)
        self._evicted_alerts_by_inv: collections.defaultdict[str, int] = collections.defaultdict(int)
        self._evicted_diagnostic_ids: collections.deque[str] = collections.deque(maxlen=self._max_diagnostics)
        self._evicted_alert_ids: collections.deque[str] = collections.deque(maxlen=self._max_alerts)
        self._unsupported_records_skipped: int = 0

        # Hydrate from StateStore if explicitly requested
        if self._state_store is not None and auto_hydrate:
            self.hydrate_from_state_store()

        if self._event_bus is not None and auto_subscribe:
            self._subscribe_to_event_bus()

    # ==============================================================================
    # EventBus Subscriptions
    # ==============================================================================

    def _subscribe_to_event_bus(self) -> None:
        """Subscribe to audit and reliability events from EventBus."""
        if self._event_bus is None:
            return

        handlers = {
            EventType.HISTORICAL_REVIEW_AUDIT_COMPLETED: self._on_audit_completed_event,
            EventType.HISTORICAL_REVIEW_AUDIT_FAILED: self._on_audit_failed_event,
            EventType.HISTORICAL_REVIEW_INTEGRITY_VIOLATION_DETECTED: self._on_integrity_violation_event,
            EventType.HISTORICAL_REVIEW_AUDIT_COVERAGE_EVALUATED: self._on_coverage_evaluated_event,
            EventType.HISTORICAL_REVIEW_AUDIT_REPEATABILITY_VERIFIED: self._on_repeatability_verified_event,
            EventType.HISTORICAL_REVIEW_AUDIT_REGRESSION_DETECTED: self._on_regression_detected_event,
            EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_FAILED: self._on_reliability_failed_event,
            EventType.HISTORICAL_REVIEW_RELIABILITY_EVALUATION_COMPLETED: self._on_reliability_completed_event,
        }

        for evt_type, handler in handlers.items():
            try:
                self._event_bus.subscribe(evt_type, handler)
            except Exception as exc:
                logger.warning("Could not subscribe to EventType '%s': %s", evt_type, exc)

    def _safe_extract_inv_id(self, payload: Dict[str, Any]) -> str:
        """Safely extract and sanitize investigation_id from event payload."""
        raw = str(payload.get("investigation_id") or "inv_unknown").strip()
        if SAFE_ID_PATTERN.match(raw) and ".." not in raw and len(raw) <= 128:
            return raw
        h = hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:10]
        return f"inv_malformed_{h}"

    def _on_audit_completed_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        status = payload.get("overall_status", "UNKNOWN")
        findings_count = payload.get("findings_count", 0)
        report_id = payload.get("report_id")

        if findings_count > 0:
            sev = AuditDiagnosticSeverity.WARNING
            cat = AuditDiagnosticCategory.GENERAL_DIAGNOSTIC
            nature = AuditEvidenceNature.OBSERVED_FAILURE
            msg = f"Audit report '{report_id}' completed with {findings_count} finding(s) for investigation '{inv_id}'."
        else:
            sev = AuditDiagnosticSeverity.INFO
            cat = AuditDiagnosticCategory.GENERAL_DIAGNOSTIC
            nature = AuditEvidenceNature.INFERRED_INTERPRETATION
            msg = f"Audit report '{report_id}' generated successfully for investigation '{inv_id}' with status '{status}'."

        self.record_diagnostic(
            investigation_id=inv_id,
            category=cat,
            severity=sev,
            evidence_nature=nature,
            message=msg,
            correlation_id=event.correlation_id,
            report_id=report_id,
            details=payload,
        )

        if status == "PASSED" and findings_count == 0:
            if self._active_failures.get(inv_id):
                self.check_and_record_recovery(
                    investigation_id=inv_id,
                    correlation_id=event.correlation_id,
                    report_id=report_id,
                )

    def _on_audit_failed_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        error_msg = payload.get("error", "Audit evaluation encountered an unhandled failure.")
        self.record_diagnostic(
            investigation_id=inv_id,
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.ERROR,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message=error_msg,
            correlation_id=event.correlation_id,
            report_id=payload.get("report_id"),
            details=payload,
            error_category=payload.get("error_category", "AuditFailure"),
        )

    def _on_integrity_violation_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        msg = payload.get("message", "Integrity violation detected in historical chain.")
        self.record_diagnostic(
            investigation_id=inv_id,
            category=AuditDiagnosticCategory.INTEGRITY_VIOLATION,
            severity=AuditDiagnosticSeverity.ERROR,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message=msg,
            correlation_id=event.correlation_id,
            report_id=payload.get("report_id"),
            details=payload,
            source_references=tuple(payload.get("source_references", ())),
        )

    def _on_coverage_evaluated_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        state = payload.get("coverage_state", "COMPLETE")
        if state in ("PARTIAL", "EMPTY", "UNAVAILABLE"):
            sev = (
                AuditDiagnosticSeverity.ERROR
                if state in ("UNAVAILABLE", "EMPTY")
                else AuditDiagnosticSeverity.WARNING
            )
            msg = f"Audit coverage degraded: {state} coverage detected for investigation '{inv_id}'."
            self.record_diagnostic(
                investigation_id=inv_id,
                category=AuditDiagnosticCategory.COVERAGE_DEGRADATION,
                severity=sev,
                evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
                message=msg,
                correlation_id=event.correlation_id,
                details=payload,
            )

    def _on_repeatability_verified_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        is_rep = payload.get("is_repeatable", True)
        if not is_rep:
            score = payload.get("repeatability_score", 0.0)
            msg = f"Audit repeatability divergence detected: score {score:.2f} < 1.0."
            self.record_diagnostic(
                investigation_id=inv_id,
                category=AuditDiagnosticCategory.REPEATABILITY_DIVERGENCE,
                severity=AuditDiagnosticSeverity.ERROR,
                evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
                message=msg,
                correlation_id=event.correlation_id,
                details=payload,
            )

    def _on_regression_detected_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        reg_state = payload.get("regression_state")
        if reg_state == "REGRESSION_DETECTED":
            new_count = payload.get("new_findings_count", 0)
            msg = f"Historical review integrity regression detected: {new_count} new findings."
            self.record_diagnostic(
                investigation_id=inv_id,
                category=AuditDiagnosticCategory.HISTORICAL_INTEGRITY_REGRESSION,
                severity=AuditDiagnosticSeverity.ERROR,
                evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
                message=msg,
                correlation_id=event.correlation_id,
                report_id=payload.get("current_report_id"),
                details=payload,
            )

    def _on_reliability_failed_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        err = payload.get("error", "Reliability evaluation failed.")
        self.record_diagnostic(
            investigation_id=inv_id,
            category=AuditDiagnosticCategory.AUDIT_EVALUATION_FAILURE,
            severity=AuditDiagnosticSeverity.CRITICAL,
            evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
            message=err,
            correlation_id=event.correlation_id,
            evaluation_id=payload.get("evaluation_id"),
            details=payload,
        )

    def _on_reliability_completed_event(self, event: Event) -> None:
        payload = event.payload or {}
        inv_id = self._safe_extract_inv_id(payload)
        status = payload.get("overall_reliability_status")
        # If successfully evaluated as RELIABLE and there were previous failures, observe recovery!
        if status == "RELIABLE":
            self.check_and_record_recovery(
                investigation_id=inv_id,
                correlation_id=event.correlation_id,
                evaluation_id=payload.get("evaluation_id"),
            )

    # ==============================================================================
    # StateStore Persistence & Hydration Lifecycle
    # ==============================================================================

    def _persist_active_failures(self) -> None:
        """Persist active failures map to StateStore."""
        if self._state_store is None:
            return
        data = {inv: [c.value for c in cats] for inv, cats in self._active_failures.items()}
        self._state_store.set("historical_review_diagnostics:active_failures", data)

    def _persist_cooldown_state(self) -> None:
        """Persist cooldown and alert suppression state to StateStore."""
        if self._state_store is None:
            return
        data = {}
        for (inv, cat, sev), (ts, alt_id) in self._last_alert_time.items():
            key = f"{inv}:{cat}:{sev}"
            data[key] = {
                "last_timestamp": ts,
                "last_alert_id": alt_id,
                "suppressed": self._suppressed_counts.get((inv, cat, sev), 0),
            }
        self._state_store.set("historical_review_diagnostics:cooldown_state", data)

    def _persist_retention_stats(self) -> None:
        """Persist eviction and retention statistics to StateStore."""
        if self._state_store is None:
            return
        data = {
            "schema_version": CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
            "evicted_diagnostics_count": self._evicted_diagnostics_count,
            "evicted_alerts_count": self._evicted_alerts_count,
            "evicted_exports_count": self._evicted_exports_count,
            "evicted_reconciliations_count": self._evicted_reconciliations_count,
            "evicted_diagnostics_by_inv": dict(self._evicted_diagnostics_by_inv),
            "evicted_alerts_by_inv": dict(self._evicted_alerts_by_inv),
            "evicted_diagnostic_ids": list(self._evicted_diagnostic_ids),
            "evicted_alert_ids": list(self._evicted_alert_ids),
            "evicted_export_ids": list(self._evicted_export_ids),
            "evicted_reconciliation_ids": list(self._evicted_reconciliation_ids),
        }
        self._state_store.set("historical_review_diagnostics:retention_stats", data)

    def hydrate_from_state_store(self, force: bool = False) -> None:
        """Hydrate in-memory diagnostic, alert, failure, cooldown, and retention state from StateStore."""
        if self._state_store is None:
            return

        with self._lock:
            # 1. Hydrate diagnostics history
            stored_diags = self._state_store.get("historical_review_diagnostics:history", [])
            if isinstance(stored_diags, list):
                if not self._diagnostics or force:
                    self._diagnostics.clear()
                    self._diagnostics_by_id.clear()
                    for item in reversed(stored_diags[: self._max_diagnostics]):
                        if isinstance(item, dict):
                            try:
                                rec = AuditDiagnosticRecord.from_dict(item)
                                self._diagnostics.append(rec)
                                self._diagnostics_by_id[rec.diagnostic_id] = rec
                            except (IncompatibleSchemaVersionError, MalformedSchemaVersionError) as exc:
                                logger.warning("Skipping unsupported/malformed diagnostic record in StateStore: %s", exc)
                                self._unsupported_records_skipped += 1
                            except Exception:
                                pass

            # 2. Hydrate alerts history
            stored_alerts = self._state_store.get("historical_review_alerts:history", [])
            if isinstance(stored_alerts, list):
                if not self._alerts or force:
                    self._alerts.clear()
                    self._suppressed_alerts.clear()
                    self._alerts_by_id.clear()
                    for item in reversed(stored_alerts[: self._max_alerts]):
                        if isinstance(item, dict):
                            try:
                                alt = AuditAlert.from_dict(item)
                                if alt.status == AuditAlertStatus.SUPPRESSED_COOLDOWN:
                                    self._suppressed_alerts.append(alt)
                                else:
                                    self._alerts.append(alt)
                                self._alerts_by_id[alt.alert_id] = alt
                            except (IncompatibleSchemaVersionError, MalformedSchemaVersionError) as exc:
                                logger.warning("Skipping unsupported/malformed alert in StateStore: %s", exc)
                                self._unsupported_records_skipped += 1
                            except Exception:
                                pass

            # Hydrate suppressed alerts if stored separately
            stored_suppressed = self._state_store.get("historical_review_alerts:suppressed_history", [])
            if isinstance(stored_suppressed, list):
                for item in stored_suppressed:
                    if isinstance(item, dict):
                        try:
                            s_alt = AuditAlert.from_dict(item)
                            if s_alt not in self._suppressed_alerts:
                                self._suppressed_alerts.append(s_alt)
                            self._alerts_by_id[s_alt.alert_id] = s_alt
                        except (IncompatibleSchemaVersionError, MalformedSchemaVersionError) as exc:
                            logger.warning("Skipping unsupported/malformed suppressed alert: %s", exc)
                            self._unsupported_records_skipped += 1
                        except Exception:
                            pass

            # 3. Hydrate active failures
            stored_fails = self._state_store.get("historical_review_diagnostics:active_failures", None)
            if isinstance(stored_fails, dict):
                self._active_failures.clear()
                for inv_id, cat_list in stored_fails.items():
                    if isinstance(cat_list, list):
                        cats = set()
                        for c_str in cat_list:
                            try:
                                cats.add(AuditDiagnosticCategory(str(c_str)))
                            except ValueError:
                                pass
                        if cats:
                            self._active_failures[str(inv_id)] = cats

            # 4. Hydrate cooldown and suppression state
            stored_cd = self._state_store.get("historical_review_diagnostics:cooldown_state", None)
            if isinstance(stored_cd, dict):
                self._last_alert_time.clear()
                self._suppressed_counts.clear()
                for key_str, cd_val in stored_cd.items():
                    if isinstance(cd_val, dict) and ":" in key_str:
                        parts = key_str.split(":", 2)
                        if len(parts) == 3:
                            inv_id, cat_str, sev_str = parts
                            try:
                                ts = float(cd_val.get("last_timestamp", 0.0))
                                alt_id = str(cd_val.get("last_alert_id", ""))
                                supp = int(cd_val.get("suppressed", 0))
                                tup = (inv_id, cat_str, sev_str)
                                self._last_alert_time[tup] = (ts, alt_id)
                                if supp > 0:
                                    self._suppressed_counts[tup] = supp
                            except (ValueError, TypeError):
                                pass

            # 5. Hydrate retention statistics
            stored_stats = self._state_store.get("historical_review_diagnostics:retention_stats", None)
            if isinstance(stored_stats, dict):
                try:
                    migrated_stats = migrate_retention_stats_payload(stored_stats)
                except (IncompatibleSchemaVersionError, MalformedSchemaVersionError) as exc:
                    logger.warning("Skipping incompatible retention stats: %s", exc)
                    migrated_stats = stored_stats

                try:
                    self._evicted_diagnostics_count = max(0, int(migrated_stats.get("evicted_diagnostics_count", 0)))
                except (ValueError, TypeError):
                    self._evicted_diagnostics_count = 0
                try:
                    self._evicted_alerts_count = max(0, int(migrated_stats.get("evicted_alerts_count", 0)))
                except (ValueError, TypeError):
                    self._evicted_alerts_count = 0
                try:
                    self._evicted_exports_count = max(0, int(migrated_stats.get("evicted_exports_count", 0)))
                except (ValueError, TypeError):
                    self._evicted_exports_count = 0
                try:
                    self._evicted_reconciliations_count = max(0, int(migrated_stats.get("evicted_reconciliations_count", 0)))
                except (ValueError, TypeError):
                    self._evicted_reconciliations_count = 0

                inv_d = migrated_stats.get("evicted_diagnostics_by_inv", {})
                if isinstance(inv_d, dict):
                    cleaned_inv_d = {}
                    for k, v in inv_d.items():
                        try:
                            cleaned_inv_d[str(k)] = max(0, int(v))
                        except (ValueError, TypeError):
                            pass
                    self._evicted_diagnostics_by_inv = collections.defaultdict(int, cleaned_inv_d)

                inv_a = migrated_stats.get("evicted_alerts_by_inv", {})
                if isinstance(inv_a, dict):
                    cleaned_inv_a = {}
                    for k, v in inv_a.items():
                        try:
                            cleaned_inv_a[str(k)] = max(0, int(v))
                        except (ValueError, TypeError):
                            pass
                    self._evicted_alerts_by_inv = collections.defaultdict(int, cleaned_inv_a)

                e_diags = migrated_stats.get("evicted_diagnostic_ids", [])
                if isinstance(e_diags, list):
                    self._evicted_diagnostic_ids = collections.deque(
                        [str(x) for x in e_diags if isinstance(x, (str, int))],
                        maxlen=self._max_diagnostics,
                    )

                e_alts = migrated_stats.get("evicted_alert_ids", [])
                if isinstance(e_alts, list):
                    self._evicted_alert_ids = collections.deque(
                        [str(x) for x in e_alts if isinstance(x, (str, int))],
                        maxlen=self._max_alerts,
                    )

                e_exps = migrated_stats.get("evicted_export_ids", [])
                if isinstance(e_exps, list):
                    self._evicted_export_ids = collections.deque(
                        [str(x) for x in e_exps if isinstance(x, (str, int))],
                        maxlen=self._max_exports,
                    )

                e_recons = migrated_stats.get("evicted_reconciliation_ids", [])
                if isinstance(e_recons, list):
                    self._evicted_reconciliation_ids = collections.deque(
                        [str(x) for x in e_recons if isinstance(x, (str, int))],
                        maxlen=self._max_reconciliations,
                    )

            # 6. Hydrate exports history
            stored_exports = self._state_store.get("historical_review_exports:history", [])
            if isinstance(stored_exports, list):
                if not self._exports or force:
                    self._exports.clear()
                    self._exports_by_id.clear()
                    for item in reversed(stored_exports[: self._max_exports]):
                        if isinstance(item, dict):
                            try:
                                exp = DiagnosticsExportResult.from_dict(item)
                                self._exports.append(exp)
                                self._exports_by_id[exp.export_id] = exp
                            except (IncompatibleSchemaVersionError, MalformedSchemaVersionError) as exc:
                                logger.warning("Skipping unsupported/malformed export: %s", exc)
                                self._unsupported_records_skipped += 1
                            except Exception:
                                pass

            # 7. Hydrate reconciliations history
            stored_recons = self._state_store.get("historical_review_reconciliations:history", [])
            if isinstance(stored_recons, list):
                if not self._reconciliations or force:
                    self._reconciliations.clear()
                    self._reconciliations_by_id.clear()
                    for item in reversed(stored_recons[: self._max_reconciliations]):
                        if isinstance(item, dict):
                            try:
                                rpt = DiagnosticsReconciliationReport.from_dict(item)
                                self._reconciliations.append(rpt)
                                self._reconciliations_by_id[rpt.reconciliation_id] = rpt
                            except (IncompatibleSchemaVersionError, MalformedSchemaVersionError) as exc:
                                logger.warning("Skipping unsupported/malformed reconciliation: %s", exc)
                                self._unsupported_records_skipped += 1
                            except Exception:
                                pass

    # ==============================================================================
    # Core Diagnostics & Alerting API
    # ==============================================================================

    def record_diagnostic(
        self,
        investigation_id: str,
        category: Union[AuditDiagnosticCategory, str],
        severity: Union[AuditDiagnosticSeverity, str],
        message: str,
        evidence_nature: Union[AuditEvidenceNature, str] = AuditEvidenceNature.OBSERVED_FAILURE,
        correlation_id: Optional[str] = None,
        report_id: Optional[str] = None,
        evaluation_id: Optional[str] = None,
        source_references: Sequence[str] = (),
        details: Optional[Dict[str, Any]] = None,
        error_category: Optional[str] = None,
    ) -> AuditDiagnosticRecord:
        """Record an immutable operational diagnostic record and evaluate alerting."""
        clean_inv = validate_safe_id(investigation_id, "investigation_id")
        now = self._time_provider()
        iso = format_utc_iso(now)

        cat_obj = category if isinstance(category, AuditDiagnosticCategory) else AuditDiagnosticCategory(str(category))
        sev_obj = severity if isinstance(severity, AuditDiagnosticSeverity) else AuditDiagnosticSeverity(str(severity))
        nat_obj = evidence_nature if isinstance(evidence_nature, AuditEvidenceNature) else AuditEvidenceNature(str(evidence_nature))

        corr_id = (
            correlation_id.strip()
            if correlation_id and SAFE_ID_PATTERN.match(correlation_id.strip())
            else f"corr_{hashlib.sha256(f'{clean_inv}:{now}'.encode()).hexdigest()[:12]}"
        )

        diag_id = compute_deterministic_diagnostic_id(clean_inv, cat_obj, now, corr_id)
        clean_msg = sanitize_text(message)
        summary_text = f"[{sev_obj.value}] {cat_obj.value} for investigation '{clean_inv}': {clean_msg}"

        safe_refs = tuple(
            str(s).strip()
            for s in source_references
            if s
            and SAFE_REF_PATTERN.match(str(s).strip())
            and ".." not in str(s)
            and "/" not in str(s)
            and "\\" not in str(s)
            and len(str(s).strip()) <= 128
        )
        prohibited_keys = {
            "api_key", "secret", "password", "token", "credential", "auth_token", "private_key",
            "prompt", "audio", "transcript", "raw_audio", "raw_transcript"
        }
        safe_details = {}
        for k, v in (details or {}).items():
            k_str = str(k).lower().strip()
            if k_str in prohibited_keys or CREDENTIAL_KEY_REGEX.search(k_str):
                continue
            if isinstance(v, (int, float, bool)):
                safe_details[str(k)] = v
            elif isinstance(v, (list, tuple)):
                safe_details[str(k)] = [sanitize_text(item) if not isinstance(item, (int, float, bool)) else item for item in v]
            elif isinstance(v, dict):
                safe_details[str(k)] = {str(dk): sanitize_text(dv) if not isinstance(dv, (int, float, bool)) else dv for dk, dv in v.items() if not CREDENTIAL_KEY_REGEX.search(str(dk).lower())}
            else:
                safe_details[str(k)] = sanitize_text(v)

        record = AuditDiagnosticRecord(
            diagnostic_id=diag_id,
            correlation_id=corr_id,
            investigation_id=clean_inv,
            timestamp=now,
            timestamp_iso=iso,
            category=cat_obj,
            severity=sev_obj,
            evidence_nature=nat_obj,
            message=clean_msg,
            report_id=report_id,
            evaluation_id=evaluation_id,
            source_references=safe_refs,
            details=safe_details,
            error_category=error_category,
            sanitized_summary=summary_text,
        )

        with self._lock:
            # 1. Store in bounded queue
            if len(self._diagnostics) >= self._max_diagnostics:
                oldest = self._diagnostics[0]
                self._diagnostics_by_id.pop(oldest.diagnostic_id, None)
                self._evicted_diagnostics_count += 1
                self._evicted_diagnostics_by_inv[oldest.investigation_id] += 1
                self._evicted_diagnostic_ids.append(oldest.diagnostic_id)
                if self._state_store is not None:
                    self._state_store.delete(f"historical_review_diagnostics:{oldest.diagnostic_id}")
                self._persist_retention_stats()

            self._diagnostics.append(record)
            self._diagnostics_by_id[record.diagnostic_id] = record

            # 2. Update failure tracking state
            if sev_obj in (AuditDiagnosticSeverity.ERROR, AuditDiagnosticSeverity.CRITICAL):
                if clean_inv not in self._active_failures:
                    self._active_failures[clean_inv] = set()
                self._active_failures[clean_inv].add(cat_obj)
                self._persist_active_failures()

            # 3. Publish Event
            if self._event_bus is not None:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_DIAGNOSTIC_RECORDED,
                        payload=record.to_dict(),
                        source="HistoricalReviewAuditDiagnosticsService",
                        correlation_id=corr_id,
                    )
                )

            # 4. StateStore Snapshot
            if self._state_store is not None:
                self._state_store.set(f"historical_review_diagnostics:{clean_inv}:latest", record.to_dict())
                self._state_store.set(f"historical_review_diagnostics:{record.diagnostic_id}", record.to_dict())

                hist = self._state_store.get("historical_review_diagnostics:history", [])
                if not isinstance(hist, list):
                    hist = []
                hist.insert(0, record.to_dict())
                self._state_store.set(
                    "historical_review_diagnostics:history",
                    hist[: self._max_diagnostics],
                )

            # 5. Evaluate Operational Alerting
            self._evaluate_alert(record)

        return record

    def _evaluate_alert(self, record: AuditDiagnosticRecord) -> Optional[AuditAlert]:
        """Evaluate whether to trigger an operational alert based on diagnostic record.

        Must be called with self._lock held.
        """
        # Only WARNING, ERROR, and CRITICAL diagnostic records warrant alerts (plus RECOVERY_OBSERVED info alert)
        should_alert = (
            record.severity in (AuditDiagnosticSeverity.WARNING, AuditDiagnosticSeverity.ERROR, AuditDiagnosticSeverity.CRITICAL)
            or record.category == AuditDiagnosticCategory.RECOVERY_OBSERVED
        )

        if not should_alert:
            return None

        inv_id = record.investigation_id
        cat_str = record.category.value
        sev_str = record.severity.value
        dedup_key = (inv_id, cat_str, sev_str)
        now = self._time_provider()

        last_info = self._last_alert_time.get(dedup_key)
        cooldown_active = False

        if last_info is not None:
            last_ts, _ = last_info
            if now < (last_ts + self._alert_cooldown_seconds):
                cooldown_active = True

        if cooldown_active:
            # Under cooldown: do NOT suppress diagnostic, but suppress duplicate alert storm
            suppressed = self._suppressed_counts.get(dedup_key, 0) + 1
            self._suppressed_counts[dedup_key] = suppressed

            cooldown_until = last_info[0] + self._alert_cooldown_seconds
            alert_id = compute_deterministic_alert_id(inv_id, record.category, now, f"suppressed_{suppressed}")
            alert = AuditAlert(
                alert_id=alert_id,
                diagnostic_id=record.diagnostic_id,
                correlation_id=record.correlation_id,
                investigation_id=inv_id,
                timestamp=now,
                timestamp_iso=format_utc_iso(now),
                category=record.category,
                severity=record.severity,
                reason=f"[COOLDOWN_SUPPRESSED x{suppressed}] {record.message}",
                source_references=record.source_references,
                status=AuditAlertStatus.SUPPRESSED_COOLDOWN,
                cooldown_until=cooldown_until,
                suppressed_occurrences=suppressed,
                report_id=record.report_id,
                evaluation_id=record.evaluation_id,
            )

            # Retain suppressed alert in dedicated suppressed ring buffer, lookup map, and StateStore
            self._suppressed_alerts.append(alert)
            self._alerts_by_id[alert.alert_id] = alert
            if self._state_store is not None:
                self._state_store.set(f"historical_review_alerts:{alert.alert_id}", alert.to_dict())
                self._state_store.set(
                    "historical_review_alerts:suppressed_history",
                    [a.to_dict() for a in self._suppressed_alerts],
                )
                self._persist_cooldown_state()

            if self._event_bus is not None:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_AUDIT_ALERT_SUPPRESSED,
                        payload=alert.to_dict(),
                        source="HistoricalReviewAuditDiagnosticsService",
                        correlation_id=record.correlation_id,
                    )
                )

            return alert

        # New alert triggered
        suppressed_so_far = self._suppressed_counts.pop(dedup_key, 0)
        cooldown_until = now + self._alert_cooldown_seconds
        alert_id = compute_deterministic_alert_id(inv_id, record.category, now, record.message)

        status = (
            AuditAlertStatus.RECOVERED
            if record.category == AuditDiagnosticCategory.RECOVERY_OBSERVED
            else AuditAlertStatus.TRIGGERED
        )

        alert = AuditAlert(
            alert_id=alert_id,
            diagnostic_id=record.diagnostic_id,
            correlation_id=record.correlation_id,
            investigation_id=inv_id,
            timestamp=now,
            timestamp_iso=format_utc_iso(now),
            category=record.category,
            severity=record.severity,
            reason=record.message,
            source_references=record.source_references,
            status=status,
            cooldown_until=cooldown_until,
            suppressed_occurrences=suppressed_so_far,
            report_id=record.report_id,
            evaluation_id=record.evaluation_id,
        )

        self._last_alert_time[dedup_key] = (now, alert.alert_id)

        if len(self._alerts) >= self._max_alerts:
            oldest = self._alerts[0]
            self._alerts_by_id.pop(oldest.alert_id, None)
            self._evicted_alerts_count += 1
            self._evicted_alerts_by_inv[oldest.investigation_id] += 1
            self._evicted_alert_ids.append(oldest.alert_id)
            if self._state_store is not None:
                self._state_store.delete(f"historical_review_alerts:{oldest.alert_id}")
            self._persist_retention_stats()

        self._alerts.append(alert)
        self._alerts_by_id[alert.alert_id] = alert

        # Publish Event
        if self._event_bus is not None:
            self._event_bus.publish(
                Event.create(
                    event_type=EventType.HISTORICAL_REVIEW_AUDIT_ALERT_TRIGGERED,
                    payload=alert.to_dict(),
                    source="HistoricalReviewAuditDiagnosticsService",
                    correlation_id=record.correlation_id,
                )
            )

        # StateStore persistence
        if self._state_store is not None:
            self._state_store.set(f"historical_review_alerts:{alert.alert_id}", alert.to_dict())
            hist = self._state_store.get("historical_review_alerts:history", [])
            if not isinstance(hist, list):
                hist = []
            hist.insert(0, alert.to_dict())
            self._state_store.set(
                "historical_review_alerts:history",
                hist[: self._max_alerts],
            )
            self._persist_cooldown_state()

        return alert

    # ==============================================================================
    # Recovery Lifecycle Tracking
    # ==============================================================================

    def check_and_record_recovery(
        self,
        investigation_id: str,
        correlation_id: Optional[str] = None,
        report_id: Optional[str] = None,
        evaluation_id: Optional[str] = None,
        target_categories: Optional[Sequence[Union[AuditDiagnosticCategory, str]]] = None,
    ) -> Optional[AuditDiagnosticRecord]:
        """Check if an investigation previously experienced failures and record an explicit recovery."""
        clean_inv = validate_safe_id(investigation_id, "investigation_id")

        with self._lock:
            active = self._active_failures.get(clean_inv)
            # Find any active failure states or active alerts
            unresolved_alert_cats = set()
            for a in self._alerts:
                if a.investigation_id == clean_inv and a.status in (
                    AuditAlertStatus.TRIGGERED,
                    AuditAlertStatus.ACTIVE,
                    AuditAlertStatus.ACKNOWLEDGED,
                ):
                    unresolved_alert_cats.add(a.category)

            available_failures = set(active or set()) | unresolved_alert_cats
            if not available_failures:
                return None

            # Determine categories to resolve
            if target_categories:
                target_cat_objs = set()
                for tc in target_categories:
                    obj = tc if isinstance(tc, AuditDiagnosticCategory) else AuditDiagnosticCategory(str(tc))
                    target_cat_objs.add(obj)
                to_resolve = available_failures.intersection(target_cat_objs)
                if not to_resolve:
                    return None
            else:
                to_resolve = set(available_failures)

            resolved_categories = sorted([c.value for c in to_resolve])

            # Update active failures
            if clean_inv in self._active_failures:
                for c in to_resolve:
                    self._active_failures[clean_inv].discard(c)
                if not self._active_failures[clean_inv]:
                    self._active_failures.pop(clean_inv, None)
            self._persist_active_failures()

            # Update matching alerts to RESOLVED
            for i, a in enumerate(list(self._alerts)):
                if a.investigation_id == clean_inv and a.category in to_resolve and a.status in (
                    AuditAlertStatus.TRIGGERED,
                    AuditAlertStatus.ACTIVE,
                    AuditAlertStatus.ACKNOWLEDGED,
                ):
                    resolved_dict = a.to_dict()
                    resolved_dict["status"] = AuditAlertStatus.RESOLVED.value
                    resolved_alert = AuditAlert.from_dict(resolved_dict)
                    self._alerts[i] = resolved_alert
                    self._alerts_by_id[a.alert_id] = resolved_alert
                    if self._state_store is not None:
                        self._state_store.set(f"historical_review_alerts:{a.alert_id}", resolved_alert.to_dict())

            # Update history list in StateStore
            if self._state_store is not None:
                hist = self._state_store.get("historical_review_alerts:history", [])
                if isinstance(hist, list):
                    updated_hist = []
                    for item in hist:
                        if (
                            isinstance(item, dict)
                            and item.get("investigation_id") == clean_inv
                            and item.get("category") in resolved_categories
                            and item.get("status") in (
                                AuditAlertStatus.TRIGGERED.value,
                                AuditAlertStatus.ACTIVE.value,
                                AuditAlertStatus.ACKNOWLEDGED.value,
                            )
                        ):
                            item_copy = dict(item)
                            item_copy["status"] = AuditAlertStatus.RESOLVED.value
                            updated_hist.append(item_copy)
                        else:
                            updated_hist.append(item)
                    self._state_store.set("historical_review_alerts:history", updated_hist)

            msg = (
                f"Operational recovery observed for investigation '{clean_inv}'. "
                f"Previously active failure states resolved: {', '.join(resolved_categories)}."
            )

            rec = self.record_diagnostic(
                investigation_id=clean_inv,
                category=AuditDiagnosticCategory.RECOVERY_OBSERVED,
                severity=AuditDiagnosticSeverity.INFO,
                evidence_nature=AuditEvidenceNature.OBSERVED_FAILURE,
                message=msg,
                correlation_id=correlation_id,
                report_id=report_id,
                evaluation_id=evaluation_id,
                details={"resolved_failure_categories": resolved_categories},
            )

            if self._event_bus is not None:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_AUDIT_RECOVERY_OBSERVED,
                        payload=rec.to_dict(),
                        source="HistoricalReviewAuditDiagnosticsService",
                        correlation_id=rec.correlation_id,
                    )
                )

            return rec

    # ==============================================================================
    # Alert Lifecycle Transitions (Acknowledge / Resolve)
    # ==============================================================================

    def acknowledge_alert(
        self,
        alert_id: str,
        acknowledged_by: Optional[str] = None,
    ) -> Optional[AuditAlert]:
        """Transition an active or triggered alert to ACKNOWLEDGED status."""
        clean_id = (alert_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            target_alert = self.get_alert(clean_id)
            if target_alert is None:
                return None

            if target_alert.status == AuditAlertStatus.RESOLVED:
                return target_alert

            updated = AuditAlert(
                alert_id=target_alert.alert_id,
                diagnostic_id=target_alert.diagnostic_id,
                correlation_id=target_alert.correlation_id,
                investigation_id=target_alert.investigation_id,
                timestamp=target_alert.timestamp,
                timestamp_iso=target_alert.timestamp_iso,
                category=target_alert.category,
                severity=target_alert.severity,
                reason=target_alert.reason,
                source_references=target_alert.source_references,
                status=AuditAlertStatus.ACKNOWLEDGED,
                cooldown_until=target_alert.cooldown_until,
                suppressed_occurrences=target_alert.suppressed_occurrences,
                report_id=target_alert.report_id,
                evaluation_id=target_alert.evaluation_id,
            )

            for i, a in enumerate(self._alerts):
                if a.alert_id == clean_id:
                    self._alerts[i] = updated
                    break
            self._alerts_by_id[clean_id] = updated

            if self._state_store is not None:
                self._state_store.set(f"historical_review_alerts:{clean_id}", updated.to_dict())
                hist = self._state_store.get("historical_review_alerts:history", [])
                if isinstance(hist, list):
                    new_hist = [updated.to_dict() if (isinstance(item, dict) and item.get("alert_id") == clean_id) else item for item in hist]
                    self._state_store.set("historical_review_alerts:history", new_hist)

            return updated

    def resolve_alert(
        self,
        alert_id: str,
        reason: Optional[str] = None,
    ) -> Optional[AuditAlert]:
        """Explicitly resolve an operational alert and update failure lifecycle tracking."""
        clean_id = (alert_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            target_alert = self.get_alert(clean_id)
            if target_alert is None:
                return None

            if target_alert.status == AuditAlertStatus.RESOLVED:
                return target_alert

            resolved_reason = target_alert.reason
            if reason:
                resolved_reason = f"{target_alert.reason} | Resolved: {sanitize_text(reason)}"

            updated = AuditAlert(
                alert_id=target_alert.alert_id,
                diagnostic_id=target_alert.diagnostic_id,
                correlation_id=target_alert.correlation_id,
                investigation_id=target_alert.investigation_id,
                timestamp=target_alert.timestamp,
                timestamp_iso=target_alert.timestamp_iso,
                category=target_alert.category,
                severity=target_alert.severity,
                reason=resolved_reason,
                source_references=target_alert.source_references,
                status=AuditAlertStatus.RESOLVED,
                cooldown_until=target_alert.cooldown_until,
                suppressed_occurrences=target_alert.suppressed_occurrences,
                report_id=target_alert.report_id,
                evaluation_id=target_alert.evaluation_id,
            )

            for i, a in enumerate(self._alerts):
                if a.alert_id == clean_id:
                    self._alerts[i] = updated
                    break
            self._alerts_by_id[clean_id] = updated

            inv_id = target_alert.investigation_id
            cat = target_alert.category
            other_active = any(
                a.investigation_id == inv_id
                and a.category == cat
                and a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE, AuditAlertStatus.ACKNOWLEDGED)
                for a in self._alerts
            )
            if not other_active and inv_id in self._active_failures:
                self._active_failures[inv_id].discard(cat)
                if not self._active_failures[inv_id]:
                    self._active_failures.pop(inv_id, None)
                self._persist_active_failures()

            if self._state_store is not None:
                self._state_store.set(f"historical_review_alerts:{clean_id}", updated.to_dict())
                hist = self._state_store.get("historical_review_alerts:history", [])
                if isinstance(hist, list):
                    new_hist = [updated.to_dict() if (isinstance(item, dict) and item.get("alert_id") == clean_id) else item for item in hist]
                    self._state_store.set("historical_review_alerts:history", new_hist)

            return updated

    # ==============================================================================
    # Eviction & Retention Query Interfaces
    # ==============================================================================

    def is_diagnostic_evicted(self, diagnostic_id: str) -> bool:
        """Check if a diagnostic record was evicted due to capacity retention boundaries."""
        with self._lock:
            if not self._evicted_diagnostic_ids and self._state_store is not None:
                self.hydrate_from_state_store()
            return diagnostic_id in self._evicted_diagnostic_ids

    def is_alert_evicted(self, alert_id: str) -> bool:
        """Check if an operational alert was evicted due to capacity retention boundaries."""
        with self._lock:
            if not self._evicted_alert_ids and self._state_store is not None:
                self.hydrate_from_state_store()
            return alert_id in self._evicted_alert_ids

    def get_retention_status(self) -> Dict[str, Any]:
        """Return system-wide retention and capacity metadata."""
        with self._lock:
            if not self._diagnostics and not self._alerts and not self._exports and self._state_store is not None:
                self.hydrate_from_state_store()
            oldest_diag_ts = self._diagnostics[0].timestamp if self._diagnostics else None
            oldest_alert_ts = self._alerts[0].timestamp if self._alerts else None
            return {
                "schema_version": CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
                "supported_schema_versions": list(sorted(SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS)),
                "unsupported_records_skipped": self._unsupported_records_skipped,
                "max_diagnostics_capacity": self._max_diagnostics,
                "max_alerts_capacity": self._max_alerts,
                "current_diagnostics_count": len(self._diagnostics),
                "current_alerts_count": len(self._alerts),
                "total_diagnostics_evicted": self._evicted_diagnostics_count,
                "total_alerts_evicted": self._evicted_alerts_count,
                "evicted_diagnostics_count": self._evicted_diagnostics_count,
                "evicted_alerts_count": self._evicted_alerts_count,
                "oldest_diagnostic_timestamp": oldest_diag_ts,
                "oldest_diagnostic_timestamp_iso": format_utc_iso(oldest_diag_ts) if oldest_diag_ts else None,
                "oldest_alert_timestamp": oldest_alert_ts,
                "oldest_alert_timestamp_iso": format_utc_iso(oldest_alert_ts) if oldest_alert_ts else None,
                "active_failures_tracked": len(self._active_failures),
                "cooldown_entries_active": len(self._last_alert_time),
                "state_store_configured": self._state_store is not None,
                "max_exports_capacity": self._max_exports,
                "current_exports_count": len(self._exports),
                "total_exports_evicted": self._evicted_exports_count,
                "max_reconciliations_capacity": self._max_reconciliations,
                "current_reconciliations_count": len(self._reconciliations),
                "total_reconciliations_evicted": self._evicted_reconciliations_count,
            }

    # ==============================================================================
    # Read-Only Query Interfaces
    # ==============================================================================

    def get_diagnostic(self, diagnostic_id: str) -> Optional[AuditDiagnosticRecord]:
        """Retrieve a specific diagnostic record by ID."""
        clean_id = (diagnostic_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._evicted_diagnostic_ids:
                return None

            if clean_id in self._diagnostics_by_id:
                return self._diagnostics_by_id[clean_id]

            if self._state_store is not None:
                item = self._state_store.get(f"historical_review_diagnostics:{clean_id}", None)
                if isinstance(item, dict):
                    try:
                        rec = AuditDiagnosticRecord.from_dict(item)
                        return rec
                    except Exception:
                        return None

            return None

    def list_diagnostics(
        self,
        investigation_id: Optional[str] = None,
        category: Optional[Union[AuditDiagnosticCategory, str]] = None,
        severity: Optional[Union[AuditDiagnosticSeverity, str]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AuditDiagnosticRecord]:
        """Query diagnostic records with optional filters and pagination."""
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)

        cat_val = category.value if isinstance(category, Enum) else (str(category) if category else None)
        sev_val = severity.value if isinstance(severity, Enum) else (str(severity) if severity else None)

        with self._lock:
            # Hydrate from StateStore if in-memory queue is empty
            if not self._diagnostics and self._state_store is not None:
                self.hydrate_from_state_store()

            results = list(self._diagnostics)

            # Filter
            if investigation_id:
                clean_inv = investigation_id.strip()
                results = [r for r in results if r.investigation_id == clean_inv]

            if cat_val:
                results = [r for r in results if r.category.value == cat_val]

            if sev_val:
                results = [r for r in results if r.severity.value == sev_val]

            # Newest first
            results.sort(key=lambda r: r.timestamp, reverse=True)
            return results[clamped_offset : clamped_offset + clamped_limit]

    def get_alert(self, alert_id: str) -> Optional[AuditAlert]:
        """Retrieve a specific operational alert by ID."""
        clean_id = (alert_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._evicted_alert_ids:
                return None

            if clean_id in self._alerts_by_id:
                return self._alerts_by_id[clean_id]

            if self._state_store is not None:
                item = self._state_store.get(f"historical_review_alerts:{clean_id}", None)
                if isinstance(item, dict):
                    try:
                        alert = AuditAlert.from_dict(item)
                        return alert
                    except Exception:
                        return None

            return None

    def list_alerts(
        self,
        investigation_id: Optional[str] = None,
        category: Optional[Union[AuditDiagnosticCategory, str]] = None,
        severity: Optional[Union[AuditDiagnosticSeverity, str]] = None,
        active_only: bool = False,
        limit: int = 50,
        offset: int = 0,
        include_suppressed: bool = False,
    ) -> List[AuditAlert]:
        """Query operational alerts with optional filters and pagination."""
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)

        cat_val = category.value if isinstance(category, Enum) else (str(category) if category else None)
        sev_val = severity.value if isinstance(severity, Enum) else (str(severity) if severity else None)

        with self._lock:
            # Hydrate from StateStore if in-memory queue is empty
            if not self._alerts and not self._suppressed_alerts and self._state_store is not None:
                self.hydrate_from_state_store()

            results = list(self._alerts)
            if include_suppressed:
                results.extend(self._suppressed_alerts)

            if investigation_id:
                clean_inv = investigation_id.strip()
                results = [a for a in results if a.investigation_id == clean_inv]

            if cat_val:
                results = [a for a in results if a.category.value == cat_val]

            if sev_val:
                results = [a for a in results if a.severity.value == sev_val]

            if active_only:
                results = [
                    a for a in results
                    if a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE, AuditAlertStatus.ACKNOWLEDGED)
                ]

            results.sort(key=lambda a: a.timestamp, reverse=True)
            return results[clamped_offset : clamped_offset + clamped_limit]

    def get_diagnostic_summary(self, investigation_id: str) -> AuditDiagnosticSummary:
        """Generate a roll-up diagnostic summary for a specific investigation."""
        clean_inv = validate_safe_id(investigation_id, "investigation_id")

        with self._lock:
            if not self._diagnostics and not self._alerts and self._state_store is not None:
                self.hydrate_from_state_store()

            matching_diags = [d for d in self._diagnostics if d.investigation_id == clean_inv]
            if not matching_diags and self._state_store is not None:
                stored_hist = self._state_store.get("historical_review_diagnostics:history", [])
                if isinstance(stored_hist, list):
                    for item in stored_hist:
                        if isinstance(item, dict) and item.get("investigation_id") == clean_inv:
                            try:
                                matching_diags.append(AuditDiagnosticRecord.from_dict(item))
                            except Exception:
                                pass

            matching_alerts = [a for a in self._alerts if a.investigation_id == clean_inv]
            if not matching_alerts and self._state_store is not None:
                stored_hist = self._state_store.get("historical_review_alerts:history", [])
                if isinstance(stored_hist, list):
                    for item in stored_hist:
                        if isinstance(item, dict) and item.get("investigation_id") == clean_inv:
                            try:
                                matching_alerts.append(AuditAlert.from_dict(item))
                            except Exception:
                                pass

            cat_counts: Dict[str, int] = collections.defaultdict(int)
            sev_counts: Dict[str, int] = collections.defaultdict(int)
            nat_counts: Dict[str, int] = collections.defaultdict(int)

            for d in matching_diags:
                cat_counts[d.category.value] += 1
                sev_counts[d.severity.value] += 1
                nat_counts[d.evidence_nature.value] += 1

            active_alerts = [
                a for a in matching_alerts
                if a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE, AuditAlertStatus.ACKNOWLEDGED)
            ]

            if matching_diags:
                matching_diags.sort(key=lambda d: d.timestamp, reverse=True)
                latest_ts = matching_diags[0].timestamp
                latest_iso = matching_diags[0].timestamp_iso
                oldest_ts = matching_diags[-1].timestamp
                oldest_iso = matching_diags[-1].timestamp_iso
            else:
                latest_ts = None
                latest_iso = None
                oldest_ts = None
                oldest_iso = None

            recovery = any(d.category == AuditDiagnosticCategory.RECOVERY_OBSERVED for d in matching_diags)
            has_active = bool(self._active_failures.get(clean_inv)) or bool(active_alerts and not recovery)
            recent_reasons = tuple(a.reason for a in matching_alerts[:5])

            evicted_diags = self._evicted_diagnostics_by_inv.get(clean_inv, 0)
            evicted_alts = self._evicted_alerts_by_inv.get(clean_inv, 0)
            is_truncated = (evicted_diags > 0 or evicted_alts > 0)
            if is_truncated:
                retention_status = "TRUNCATED_TO_CAPACITY"
            elif not matching_diags and not matching_alerts:
                retention_status = "NO_DATA"
            else:
                retention_status = "COMPLETE"

            return AuditDiagnosticSummary(
                investigation_id=clean_inv,
                total_diagnostics=len(matching_diags),
                total_alerts=len(matching_alerts),
                active_alerts_count=len(active_alerts),
                category_counts=dict(cat_counts),
                severity_counts=dict(sev_counts),
                evidence_nature_counts=dict(nat_counts),
                latest_timestamp=latest_ts,
                latest_timestamp_iso=latest_iso,
                has_active_failures=has_active,
                recovery_observed=recovery,
                recent_alert_reasons=recent_reasons,
                retention_status=retention_status,
                is_truncated=is_truncated,
                evicted_diagnostics_count=evicted_diags,
                evicted_alerts_count=evicted_alts,
                oldest_timestamp=oldest_ts,
                oldest_timestamp_iso=oldest_iso,
                capacity=self._max_diagnostics,
            )

    # ==============================================================================
    # Historical Review Diagnostics Auditability, Export & Reconciliation (Phase 2, Step 40)
    # ==============================================================================

    def export_diagnostics(
        self,
        request: Optional[DiagnosticsExportRequest] = None,
        correlation_id: Optional[str] = None,
    ) -> DiagnosticsExportResult:
        """Create a deterministic, read-only snapshot export of diagnostic records and alerts."""
        req = request or DiagnosticsExportRequest()
        clean_inv = None
        if req.investigation_id:
            clean_inv = validate_safe_id(req.investigation_id, "investigation_id")

        with self._lock:
            # 1. Hydrate if empty and store present
            if not self._diagnostics and not self._alerts and self._state_store is not None:
                self.hydrate_from_state_store()

            # 2. Filter diagnostics
            matching_diags = list(self._diagnostics)
            if clean_inv:
                matching_diags = [d for d in matching_diags if d.investigation_id == clean_inv]
            if req.category:
                matching_diags = [d for d in matching_diags if d.category == req.category]
            if req.severity:
                matching_diags = [d for d in matching_diags if d.severity == req.severity]

            # 3. Filter alerts
            total_matching_alerts = 0
            matching_alerts: List[AuditAlert] = []
            matching_suppressed_alerts: List[AuditAlert] = []
            if req.include_alerts:
                alert_pool = list(self._alerts)
                if clean_inv:
                    alert_pool = [a for a in alert_pool if a.investigation_id == clean_inv]
                if req.category:
                    alert_pool = [a for a in alert_pool if a.category == req.category]
                if req.severity:
                    alert_pool = [a for a in alert_pool if a.severity == req.severity]

                # Count suppressed alerts
                supp_pool = list(self._suppressed_alerts)
                if clean_inv:
                    supp_pool = [a for a in supp_pool if a.investigation_id == clean_inv]
                if req.category:
                    supp_pool = [a for a in supp_pool if a.category == req.category]
                if req.severity:
                    supp_pool = [a for a in supp_pool if a.severity == req.severity]
                matching_suppressed_alerts = supp_pool

                if req.include_suppressed:
                    alert_pool.extend(supp_pool)

                if req.active_only:
                    alert_pool = [
                        a for a in alert_pool
                        if a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE, AuditAlertStatus.ACKNOWLEDGED)
                    ]

                total_matching_alerts = len(alert_pool)
                matching_alerts = alert_pool
            else:
                alert_pool = list(self._alerts)
                if clean_inv:
                    alert_pool = [a for a in alert_pool if a.investigation_id == clean_inv]
                total_matching_alerts = len(alert_pool)

            # 4. Limits and omission tracking
            clamped_limit = max(1, min(1000, req.limit))
            omitted_reasons: Dict[str, int] = collections.defaultdict(int)

            if len(matching_diags) > clamped_limit:
                omitted_reasons["diagnostics_limit_exceeded"] = len(matching_diags) - clamped_limit
                final_diags = matching_diags[:clamped_limit]
            else:
                final_diags = matching_diags

            if len(matching_alerts) > clamped_limit:
                omitted_reasons["alerts_limit_exceeded"] = len(matching_alerts) - clamped_limit
                final_alerts = matching_alerts[:clamped_limit]
            else:
                final_alerts = matching_alerts

            if not req.include_alerts and total_matching_alerts > 0:
                omitted_reasons["alerts_excluded_by_request"] = total_matching_alerts

            if req.include_alerts and not req.include_suppressed and matching_suppressed_alerts:
                omitted_reasons["suppressed_alerts_excluded"] = len(matching_suppressed_alerts)

            # 5. Extract source references
            ref_set: Set[str] = set()
            for d in final_diags:
                for r in d.source_references:
                    if r:
                        ref_set.add(str(r))
                if req.include_audit_references and d.report_id:
                    ref_set.add(str(d.report_id))
                if req.include_reliability_references and d.evaluation_id:
                    ref_set.add(str(d.evaluation_id))

            for a in final_alerts:
                for r in a.source_references:
                    if r:
                        ref_set.add(str(r))
                if req.include_audit_references and a.report_id:
                    ref_set.add(str(a.report_id))
                if req.include_reliability_references and a.evaluation_id:
                    ref_set.add(str(a.evaluation_id))

            sorted_refs = tuple(sorted(ref_set))

            # 6. Retention and truncation status
            if clean_inv:
                evicted_d = self._evicted_diagnostics_by_inv.get(clean_inv, 0)
                evicted_a = self._evicted_alerts_by_inv.get(clean_inv, 0)
            else:
                evicted_d = self._evicted_diagnostics_count
                evicted_a = self._evicted_alerts_count

            total_evicted = evicted_d + evicted_a
            if total_evicted > 0:
                omitted_reasons["evicted_due_to_retention"] = total_evicted

            if total_evicted > 0:
                retention_status = "TRUNCATED_TO_CAPACITY"
                is_truncated = True
            elif (
                omitted_reasons.get("diagnostics_limit_exceeded", 0) > 0
                or omitted_reasons.get("alerts_limit_exceeded", 0) > 0
            ):
                retention_status = "TRUNCATED_TO_LIMIT"
                is_truncated = True
            elif not final_diags and not final_alerts:
                retention_status = "NO_DATA"
                is_truncated = False
            else:
                retention_status = "COMPLETE"
                is_truncated = False

            # 7. Canonical sorting of records for deterministic snapshot
            sorted_diags = tuple(sorted(final_diags, key=lambda d: (d.timestamp, d.diagnostic_id)))
            sorted_alerts = tuple(sorted(final_alerts, key=lambda a: (a.timestamp, a.alert_id)))

            # 8. Compute canonical dict, fingerprint, and deterministic export ID
            can_dict = compute_export_canonical_dict(
                schema_version=req.schema_version,
                investigation_id=clean_inv,
                diagnostics=sorted_diags,
                alerts=sorted_alerts,
                source_references=sorted_refs,
                retention_status=retention_status,
                is_truncated=is_truncated,
            )
            content_fingerprint = compute_export_content_fingerprint(can_dict)
            created_at = self._time_provider()
            created_at_iso = format_utc_iso(created_at)
            export_id = compute_deterministic_export_id(created_at, content_fingerprint)

            export_result = DiagnosticsExportResult(
                export_id=export_id,
                request=req,
                created_at=created_at,
                created_at_iso=created_at_iso,
                schema_version=req.schema_version,
                diagnostics=sorted_diags,
                alerts=sorted_alerts,
                record_counts={
                    "diagnostics": len(sorted_diags),
                    "alerts": len(sorted_alerts),
                },
                source_references=sorted_refs,
                retention_status=retention_status,
                is_truncated=is_truncated,
                content_fingerprint=content_fingerprint,
                omitted_reasons=dict(omitted_reasons),
            )

            # 9. In-memory bounded storage & eviction tracking
            if len(self._exports) == self._exports.maxlen:
                evicted_exp = self._exports[0]
                remaining_same_id = any(e.export_id == evicted_exp.export_id for e in list(self._exports)[1:])
                if not remaining_same_id:
                    self._evicted_export_ids.append(evicted_exp.export_id)
                    self._evicted_exports_count += 1
                    self._exports_by_id.pop(evicted_exp.export_id, None)
                    if self._state_store is not None:
                        self._state_store.delete(f"historical_review_exports:{evicted_exp.export_id}")
                    self._persist_retention_stats()

            if export_id in self._evicted_export_ids:
                try:
                    self._evicted_export_ids.remove(export_id)
                except ValueError:
                    pass

            self._exports.append(export_result)
            self._exports_by_id[export_id] = export_result

            # 10. Persist metadata into StateStore if configured
            if self._state_store is not None:
                self._state_store.set(f"historical_review_exports:{export_id}", export_result.to_dict())
                exp_history = self._state_store.get("historical_review_exports:history", [])
                if not isinstance(exp_history, list):
                    exp_history = []
                updated_exp_history = [export_result.to_dict()] + [
                    h for h in exp_history if isinstance(h, dict) and h.get("export_id") != export_id
                ]
                self._state_store.set(
                    "historical_review_exports:history",
                    updated_exp_history[: self._max_exports],
                )

            # 11. Publish EventBus event
            if self._event_bus is not None:
                try:
                    self._event_bus.publish(
                        Event.create(
                            event_type=EventType.HISTORICAL_REVIEW_DIAGNOSTICS_EXPORTED,
                            source="HistoricalReviewAuditDiagnosticsService",
                            correlation_id=correlation_id or export_id,
                            payload={
                                "export_id": export_id,
                                "investigation_id": clean_inv,
                                "record_counts": export_result.record_counts,
                                "content_fingerprint": content_fingerprint,
                                "retention_status": retention_status,
                                "is_truncated": is_truncated,
                                "schema_version": req.schema_version,
                                "created_at": created_at,
                                "created_at_iso": created_at_iso,
                            },
                        )
                    )
                except Exception as exc:
                    logger.warning("Failed to publish HISTORICAL_REVIEW_DIAGNOSTICS_EXPORTED event: %s", exc)

            return export_result

    def get_export(self, export_id: str) -> Optional[DiagnosticsExportResult]:
        """Retrieve an export snapshot by ID."""
        clean_id = (export_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._exports_by_id:
                return self._exports_by_id[clean_id]

            if clean_id in self._evicted_export_ids:
                return None

            if self._state_store is not None:
                item = self._state_store.get(f"historical_review_exports:{clean_id}", None)
                if isinstance(item, dict):
                    try:
                        exp = DiagnosticsExportResult.from_dict(item)
                        return exp
                    except Exception:
                        return None

            return None

    def list_exports(
        self,
        investigation_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[DiagnosticsExportResult]:
        """List historical review diagnostic exports with optional filters and pagination."""
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)

        with self._lock:
            if not self._exports and self._state_store is not None:
                self.hydrate_from_state_store()

            results = list(self._exports)
            if investigation_id:
                clean_inv = investigation_id.strip()
                results = [e for e in results if e.request.investigation_id == clean_inv]

            results.sort(key=lambda e: e.created_at, reverse=True)
            return results[clamped_offset : clamped_offset + clamped_limit]

    def reconcile_export(
        self,
        export_id: Union[str, DiagnosticsExportResult],
        correlation_id: Optional[str] = None,
    ) -> DiagnosticsReconciliationReport:
        """Reconcile exported diagnostic and alert records against currently active source records."""
        if isinstance(export_id, DiagnosticsExportResult):
            export_obj: Optional[DiagnosticsExportResult] = export_id
            clean_id = validate_safe_id(export_obj.export_id, "export_id")
        else:
            clean_id = validate_safe_id(export_id, "export_id")
            export_obj = None

        recon_ts = self._time_provider()
        recon_ts_iso = format_utc_iso(recon_ts)
        recon_id = compute_deterministic_reconciliation_id(clean_id, recon_ts)

        with self._lock:
            if not self._diagnostics and not self._alerts and self._state_store is not None:
                self.hydrate_from_state_store()

            # 1. Retrieve export snapshot
            export = export_obj if export_obj is not None else self.get_export(clean_id)
            if export is None:
                if self._state_store is not None:
                    raw_stored = self._state_store.get(f"historical_review_exports:{clean_id}", None)
                    if isinstance(raw_stored, dict):
                        raw_v = raw_stored.get("schema_version")
                        c_status, v_detected = detect_and_validate_schema_version(raw_v)
                        if c_status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
                            report = DiagnosticsReconciliationReport(
                                reconciliation_id=recon_id,
                                export_id=clean_id,
                                timestamp=recon_ts,
                                timestamp_iso=recon_ts_iso,
                                status=DiagnosticsReconciliationStatus.UNVERIFIABLE,
                                total_exported_diagnostics=len(raw_stored.get("diagnostics", [])),
                                total_exported_alerts=len(raw_stored.get("alerts", [])),
                                matched_diagnostics=0,
                                matched_alerts=0,
                                discrepancies=(),
                                source_retention_status="NO_DATA",
                                is_source_truncated=False,
                                fingerprint_verified=False,
                                summary_notes=f"Export record '{clean_id}' has unsupported future schema version '{v_detected}'. Supported versions: {list(sorted(SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS))}.",
                                schema_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
                            )
                            self._record_reconciliation_report(report, correlation_id)
                            return report

                is_evicted = self.is_export_evicted(clean_id)
                status = (
                    DiagnosticsReconciliationStatus.UNAVAILABLE
                    if is_evicted
                    else DiagnosticsReconciliationStatus.UNAVAILABLE
                )
                note = (
                    f"Export record '{clean_id}' was evicted due to retention limits."
                    if is_evicted
                    else f"Export record '{clean_id}' not found."
                )
                report = DiagnosticsReconciliationReport(
                    reconciliation_id=recon_id,
                    export_id=clean_id,
                    timestamp=recon_ts,
                    timestamp_iso=recon_ts_iso,
                    status=status,
                    total_exported_diagnostics=0,
                    total_exported_alerts=0,
                    matched_diagnostics=0,
                    matched_alerts=0,
                    discrepancies=(),
                    source_retention_status="UNKNOWN",
                    is_source_truncated=False,
                    fingerprint_verified=False,
                    summary_notes=note,
                    schema_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
                )
                self._record_reconciliation_report(report, correlation_id)
                return report

            # Check schema version compatibility of export
            compat_status, ver_str = detect_and_validate_schema_version(export.schema_version)
            if compat_status == SchemaVersionCompatibilityStatus.UNSUPPORTED_FUTURE:
                report = DiagnosticsReconciliationReport(
                    reconciliation_id=recon_id,
                    export_id=clean_id,
                    timestamp=recon_ts,
                    timestamp_iso=recon_ts_iso,
                    status=DiagnosticsReconciliationStatus.UNVERIFIABLE,
                    total_exported_diagnostics=len(export.diagnostics),
                    total_exported_alerts=len(export.alerts),
                    matched_diagnostics=0,
                    matched_alerts=0,
                    discrepancies=(),
                    source_retention_status="NO_DATA",
                    is_source_truncated=False,
                    fingerprint_verified=False,
                    summary_notes=f"Export record '{clean_id}' has unsupported future schema version '{export.schema_version}'. Supported versions: {list(sorted(SUPPORTED_DIAGNOSTICS_SCHEMA_VERSIONS))}.",
                    schema_version=CURRENT_DIAGNOSTICS_SCHEMA_VERSION,
                )
                self._record_reconciliation_report(report, correlation_id)
                return report

            # 2. Verify content fingerprint
            can_dict = compute_export_canonical_dict(
                schema_version=export.schema_version,
                investigation_id=export.request.investigation_id,
                diagnostics=export.diagnostics,
                alerts=export.alerts,
                source_references=export.source_references,
                retention_status=export.retention_status,
                is_truncated=export.is_truncated,
            )
            recalculated_fp = compute_export_content_fingerprint(can_dict)
            fp_verified = (recalculated_fp == export.content_fingerprint)

            discrepancies: List[DiagnosticsReconciliationDiscrepancy] = []
            if not fp_verified:
                discrepancies.append(
                    DiagnosticsReconciliationDiscrepancy(
                        discrepancy_type="FINGERPRINT_MISMATCH",
                        record_id=export.export_id,
                        investigation_id=export.request.investigation_id,
                        description="Export content fingerprint mismatch detected during reconciliation.",
                        expected=export.content_fingerprint,
                        actual=recalculated_fp,
                        is_retention_limitation=False,
                    )
                )

            # 3. Compare diagnostics
            seen_diags: Set[str] = set()
            matched_diags = 0
            for d in export.diagnostics:
                if d.diagnostic_id in seen_diags:
                    discrepancies.append(
                        DiagnosticsReconciliationDiscrepancy(
                            discrepancy_type="DUPLICATED_EXPORT_RECORD",
                            record_id=d.diagnostic_id,
                            investigation_id=d.investigation_id,
                            description=f"Export contains duplicate diagnostic ID '{d.diagnostic_id}'.",
                            expected="UNIQUE_RECORD",
                            actual="DUPLICATE",
                            is_retention_limitation=False,
                        )
                    )
                seen_diags.add(d.diagnostic_id)

                if self.is_diagnostic_evicted(d.diagnostic_id):
                    discrepancies.append(
                        DiagnosticsReconciliationDiscrepancy(
                            discrepancy_type="EVICTED_SOURCE_RECORD",
                            record_id=d.diagnostic_id,
                            investigation_id=d.investigation_id,
                            description="Source diagnostic record was evicted due to retention boundary limits.",
                            expected="ACTIVE_RECORD",
                            actual="EVICTED",
                            is_retention_limitation=True,
                        )
                    )
                else:
                    src_diag = self.get_diagnostic(d.diagnostic_id)
                    if src_diag is not None:
                        # Compare essential diagnostic attributes
                        if (
                            src_diag.category != d.category
                            or src_diag.severity != d.severity
                            or src_diag.evidence_nature != d.evidence_nature
                            or src_diag.message != d.message
                            or abs(src_diag.timestamp - d.timestamp) > 1e-6
                        ):
                            discrepancies.append(
                                DiagnosticsReconciliationDiscrepancy(
                                    discrepancy_type="ALTERED_RECORD",
                                    record_id=d.diagnostic_id,
                                    investigation_id=d.investigation_id,
                                    description="Diagnostic record content differs between export and active source.",
                                    expected=d.to_dict(),
                                    actual=src_diag.to_dict(),
                                    is_retention_limitation=False,
                                )
                            )
                        else:
                            matched_diags += 1
                    else:
                        discrepancies.append(
                            DiagnosticsReconciliationDiscrepancy(
                                discrepancy_type="MISSING_SOURCE_RECORD",
                                record_id=d.diagnostic_id,
                                investigation_id=d.investigation_id,
                                description="Exported diagnostic record was not found in active source state.",
                                expected="ACTIVE_RECORD",
                                actual="MISSING",
                                is_retention_limitation=False,
                            )
                        )

            # 4. Compare alerts
            seen_alerts: Set[str] = set()
            matched_alerts = 0
            for a in export.alerts:
                if a.alert_id in seen_alerts:
                    discrepancies.append(
                        DiagnosticsReconciliationDiscrepancy(
                            discrepancy_type="DUPLICATED_EXPORT_RECORD",
                            record_id=a.alert_id,
                            investigation_id=a.investigation_id,
                            description=f"Export contains duplicate alert ID '{a.alert_id}'.",
                            expected="UNIQUE_RECORD",
                            actual="DUPLICATE",
                            is_retention_limitation=False,
                        )
                    )
                seen_alerts.add(a.alert_id)

                if self.is_alert_evicted(a.alert_id):
                    discrepancies.append(
                        DiagnosticsReconciliationDiscrepancy(
                            discrepancy_type="EVICTED_SOURCE_RECORD",
                            record_id=a.alert_id,
                            investigation_id=a.investigation_id,
                            description="Source alert record was evicted due to retention boundary limits.",
                            expected="ACTIVE_RECORD",
                            actual="EVICTED",
                            is_retention_limitation=True,
                        )
                    )
                else:
                    src_alert = self.get_alert(a.alert_id)
                    if src_alert is not None:
                        # Core fields comparison
                        if (
                            src_alert.category != a.category
                            or src_alert.severity != a.severity
                            or abs(src_alert.timestamp - a.timestamp) > 1e-6
                        ):
                            discrepancies.append(
                                DiagnosticsReconciliationDiscrepancy(
                                    discrepancy_type="ALTERED_RECORD",
                                    record_id=a.alert_id,
                                    investigation_id=a.investigation_id,
                                    description="Alert core fields differ between export and active source.",
                                    expected=a.to_dict(),
                                    actual=src_alert.to_dict(),
                                    is_retention_limitation=False,
                                )
                            )
                        else:
                            matched_alerts += 1
                    else:
                        discrepancies.append(
                            DiagnosticsReconciliationDiscrepancy(
                                discrepancy_type="MISSING_SOURCE_RECORD",
                                record_id=a.alert_id,
                                investigation_id=a.investigation_id,
                                description="Exported alert record was not found in active source state.",
                                expected="ACTIVE_RECORD",
                                actual="MISSING",
                                is_retention_limitation=False,
                            )
                        )

            # 5. Check active retention state
            cur_ret = self.get_retention_status()
            src_evicted_count = cur_ret.get("total_diagnostics_evicted", 0) + cur_ret.get("total_alerts_evicted", 0)
            is_src_truncated = src_evicted_count > 0
            if is_src_truncated:
                src_ret_status = "TRUNCATED_TO_CAPACITY"
            elif cur_ret.get("current_diagnostics_count", 0) == 0 and cur_ret.get("current_alerts_count", 0) == 0:
                src_ret_status = "NO_DATA"
            else:
                src_ret_status = "COMPLETE"

            # 6. Evaluate reconciliation status
            has_corruption = any(not d.is_retention_limitation for d in discrepancies)
            has_retention_limitation = any(d.is_retention_limitation for d in discrepancies) or export.is_truncated

            if has_corruption:
                final_status = DiagnosticsReconciliationStatus.MISMATCHED
            elif has_retention_limitation:
                final_status = DiagnosticsReconciliationStatus.INCOMPLETE
            elif (
                matched_diags == len(export.diagnostics)
                and matched_alerts == len(export.alerts)
                and fp_verified
            ):
                final_status = DiagnosticsReconciliationStatus.MATCHED
            else:
                final_status = DiagnosticsReconciliationStatus.UNVERIFIABLE

            note = (
                f"Reconciliation {final_status.value}: "
                f"{matched_diags}/{len(export.diagnostics)} diagnostics matched, "
                f"{matched_alerts}/{len(export.alerts)} alerts matched. "
                f"{len(discrepancies)} discrepancy(ies) detected."
            )

            report = DiagnosticsReconciliationReport(
                reconciliation_id=recon_id,
                export_id=clean_id,
                timestamp=recon_ts,
                timestamp_iso=recon_ts_iso,
                status=final_status,
                total_exported_diagnostics=len(export.diagnostics),
                total_exported_alerts=len(export.alerts),
                matched_diagnostics=matched_diags,
                matched_alerts=matched_alerts,
                discrepancies=tuple(discrepancies),
                source_retention_status=src_ret_status,
                is_source_truncated=is_src_truncated,
                fingerprint_verified=fp_verified,
                summary_notes=note,
            )

            self._record_reconciliation_report(report, correlation_id)
            return report

    def _record_reconciliation_report(
        self,
        report: DiagnosticsReconciliationReport,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Store and publish a reconciliation report."""
        if len(self._reconciliations) == self._reconciliations.maxlen:
            evicted = self._reconciliations[0]
            remaining_same_rec = any(r.reconciliation_id == evicted.reconciliation_id for r in list(self._reconciliations)[1:])
            if not remaining_same_rec:
                self._evicted_reconciliation_ids.append(evicted.reconciliation_id)
                self._evicted_reconciliations_count += 1
                self._reconciliations_by_id.pop(evicted.reconciliation_id, None)
                if self._state_store is not None:
                    self._state_store.delete(f"historical_review_reconciliations:{evicted.reconciliation_id}")
                self._persist_retention_stats()

        if report.reconciliation_id in self._evicted_reconciliation_ids:
            try:
                self._evicted_reconciliation_ids.remove(report.reconciliation_id)
            except ValueError:
                pass

        self._reconciliations.append(report)
        self._reconciliations_by_id[report.reconciliation_id] = report

        if self._state_store is not None:
            self._state_store.set(f"historical_review_reconciliations:{report.reconciliation_id}", report.to_dict())
            rec_history = self._state_store.get("historical_review_reconciliations:history", [])
            if not isinstance(rec_history, list):
                rec_history = []
            updated_rec_history = [report.to_dict()] + [
                h for h in rec_history if isinstance(h, dict) and h.get("reconciliation_id") != report.reconciliation_id
            ]
            self._state_store.set(
                "historical_review_reconciliations:history",
                updated_rec_history[: self._max_reconciliations],
            )

        if self._event_bus is not None:
            try:
                self._event_bus.publish(
                    Event.create(
                        event_type=EventType.HISTORICAL_REVIEW_DIAGNOSTICS_RECONCILED,
                        source="HistoricalReviewAuditDiagnosticsService",
                        correlation_id=correlation_id or report.reconciliation_id,
                        payload={
                            "reconciliation_id": report.reconciliation_id,
                            "export_id": report.export_id,
                            "status": report.status.value,
                            "matched_diagnostics": report.matched_diagnostics,
                            "matched_alerts": report.matched_alerts,
                            "discrepancies_count": len(report.discrepancies),
                            "fingerprint_verified": report.fingerprint_verified,
                            "timestamp": report.timestamp,
                            "timestamp_iso": report.timestamp_iso,
                        },
                    )
                )
            except Exception as exc:
                logger.warning("Failed to publish HISTORICAL_REVIEW_DIAGNOSTICS_RECONCILED event: %s", exc)

    def get_reconciliation(self, reconciliation_id: str) -> Optional[DiagnosticsReconciliationReport]:
        """Retrieve a reconciliation report by ID."""
        clean_id = (reconciliation_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._reconciliations_by_id:
                return self._reconciliations_by_id[clean_id]

            if clean_id in self._evicted_reconciliation_ids:
                return None

            if self._state_store is not None:
                item = self._state_store.get(f"historical_review_reconciliations:{clean_id}", None)
                if isinstance(item, dict):
                    try:
                        rpt = DiagnosticsReconciliationReport.from_dict(item)
                        return rpt
                    except Exception:
                        return None

            return None

    def get_reconciliation_report(self, reconciliation_id: str) -> Optional[DiagnosticsReconciliationReport]:
        """Alias for get_reconciliation to retrieve a reconciliation report by ID."""
        return self.get_reconciliation(reconciliation_id)

    def list_reconciliations(
        self,
        export_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[DiagnosticsReconciliationReport]:
        """List historical review diagnostic reconciliation reports."""
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)

        with self._lock:
            if not self._reconciliations and self._state_store is not None:
                self.hydrate_from_state_store()

            results = list(self._reconciliations)
            if export_id:
                clean_exp = export_id.strip()
                results = [r for r in results if r.export_id == clean_exp]

            results.sort(key=lambda r: r.timestamp, reverse=True)
            return results[clamped_offset : clamped_offset + clamped_limit]

    def is_export_evicted(self, export_id: str) -> bool:
        """Check if an export record was evicted due to retention bounds."""
        with self._lock:
            if not self._evicted_export_ids and self._state_store is not None:
                self.hydrate_from_state_store()
            if export_id in self._exports_by_id:
                return False
            return export_id in self._evicted_export_ids

    def is_reconciliation_evicted(self, reconciliation_id: str) -> bool:
        """Check if a reconciliation report was evicted due to retention bounds."""
        with self._lock:
            if not self._evicted_reconciliation_ids and self._state_store is not None:
                self.hydrate_from_state_store()
            if reconciliation_id in self._reconciliations_by_id:
                return False
            return reconciliation_id in self._evicted_reconciliation_ids

    def clear_history(self) -> None:
        """Clear in-memory diagnostics and alerts history (test utility)."""
        with self._lock:
            self._diagnostics.clear()
            self._diagnostics_by_id.clear()
            self._alerts.clear()
            self._suppressed_alerts.clear()
            self._alerts_by_id.clear()
            self._exports.clear()
            self._exports_by_id.clear()
            self._evicted_export_ids.clear()
            self._reconciliations.clear()
            self._reconciliations_by_id.clear()
            self._evicted_reconciliation_ids.clear()
            self._last_alert_time.clear()
            self._suppressed_counts.clear()
            self._active_failures.clear()
            self._unsupported_records_skipped = 0
            self._evicted_diagnostics_count = 0
            self._evicted_alerts_count = 0
            self._evicted_exports_count = 0
            self._evicted_reconciliations_count = 0
            self._evicted_diagnostics_by_inv.clear()
            self._evicted_alerts_by_inv.clear()
            self._evicted_diagnostic_ids.clear()
            self._evicted_alert_ids.clear()
            if self._state_store is not None:
                self._state_store.delete("historical_review_diagnostics:history")
                self._state_store.delete("historical_review_alerts:history")
                self._state_store.delete("historical_review_alerts:suppressed_history")
                self._state_store.delete("historical_review_diagnostics:active_failures")
                self._state_store.delete("historical_review_diagnostics:cooldown_state")
                self._state_store.delete("historical_review_diagnostics:retention_stats")
                self._state_store.delete("historical_review_exports:history")
                self._state_store.delete("historical_review_reconciliations:history")
