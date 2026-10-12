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
    SAFE_ID_PATTERN,
    compute_deterministic_alert_id,
    compute_deterministic_diagnostic_id,
    format_utc_iso,
    sanitize_text,
    validate_safe_id,
)
from app.core.events.bus import EventBus
from app.core.events.event import Event, EventType
from app.core.exceptions import ValidationError
from app.core.state import StateStore

logger = logging.getLogger("kaiser.analysis.evolution.audit.diagnostics")

DEFAULT_MAX_DIAGNOSTICS = 100
DEFAULT_MAX_ALERTS = 100
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
        alert_cooldown_seconds: float = DEFAULT_ALERT_COOLDOWN_SECONDS,
        time_provider: Optional[Callable[[], float]] = None,
        auto_subscribe: bool = True,
    ) -> None:
        """Initialize the diagnostics and alerting service.

        Args:
            event_bus: Optional EventBus for receiving and publishing events.
            state_store: Optional StateStore for persistent snapshot storage.
            max_diagnostics: Maximum number of diagnostic records to retain.
            max_alerts: Maximum number of alert records to retain.
            alert_cooldown_seconds: Minimum seconds before re-triggering an identical alert.
            time_provider: Injectable timestamp function (defaults to time.time).
            auto_subscribe: If True and event_bus provided, automatically subscribes to audit events.
        """
        self._event_bus = event_bus
        self._state_store = state_store
        self._max_diagnostics = max(10, min(1000, max_diagnostics))
        self._max_alerts = max(10, min(1000, max_alerts))
        self._alert_cooldown_seconds = max(0.0, alert_cooldown_seconds)
        self._time_provider = time_provider or time.time
        self._lock = threading.RLock()

        # In-memory storage structures
        self._diagnostics: collections.deque[AuditDiagnosticRecord] = collections.deque(
            maxlen=self._max_diagnostics
        )
        self._diagnostics_by_id: Dict[str, AuditDiagnosticRecord] = {}

        self._alerts: collections.deque[AuditAlert] = collections.deque(maxlen=self._max_alerts)
        self._alerts_by_id: Dict[str, AuditAlert] = {}

        # Alert deduplication and cooldown tracking: (investigation_id, category, severity) -> (last_timestamp, last_alert_id)
        self._last_alert_time: Dict[Tuple[str, str, str], Tuple[float, str]] = {}
        self._suppressed_counts: Dict[Tuple[str, str, str], int] = {}

        # Failure lifecycle tracking: investigation_id -> Set of active failure categories
        self._active_failures: Dict[str, Set[AuditDiagnosticCategory]] = {}

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
            elif isinstance(v, (dict, list)):
                continue
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

            self._diagnostics.append(record)
            self._diagnostics_by_id[record.diagnostic_id] = record

            # 2. Update failure tracking state
            if sev_obj in (AuditDiagnosticSeverity.ERROR, AuditDiagnosticSeverity.CRITICAL):
                if clean_inv not in self._active_failures:
                    self._active_failures[clean_inv] = set()
                self._active_failures[clean_inv].add(cat_obj)

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
            )

            # Retain in lookup map and StateStore so get_alert(alert_id) can retrieve it
            self._alerts_by_id[alert.alert_id] = alert
            if self._state_store is not None:
                self._state_store.set(f"historical_review_alerts:{alert.alert_id}", alert.to_dict())

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
        )

        self._last_alert_time[dedup_key] = (now, alert.alert_id)

        if len(self._alerts) >= self._max_alerts:
            oldest = self._alerts[0]
            self._alerts_by_id.pop(oldest.alert_id, None)

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
    ) -> Optional[AuditDiagnosticRecord]:
        """Check if an investigation previously experienced failures and record an explicit recovery."""
        clean_inv = validate_safe_id(investigation_id, "investigation_id")

        with self._lock:
            active = self._active_failures.get(clean_inv)
            if not active:
                return None

            resolved_categories = [c.value for c in active]
            self._active_failures.pop(clean_inv, None)

            # Update previously active alerts for this investigation to RESOLVED
            for i, a in enumerate(list(self._alerts)):
                if a.investigation_id == clean_inv and a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE):
                    resolved_dict = a.to_dict()
                    resolved_dict["status"] = AuditAlertStatus.RESOLVED.value
                    resolved_alert = AuditAlert.from_dict(resolved_dict)
                    self._alerts[i] = resolved_alert
                    self._alerts_by_id[a.alert_id] = resolved_alert
                    if self._state_store is not None:
                        self._state_store.set(f"historical_review_alerts:{a.alert_id}", resolved_alert.to_dict())

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
    # Read-Only Query Interfaces
    # ==============================================================================

    def get_diagnostic(self, diagnostic_id: str) -> Optional[AuditDiagnosticRecord]:
        """Retrieve a specific diagnostic record by ID."""
        clean_id = (diagnostic_id or "").strip()
        if not clean_id or not SAFE_ID_PATTERN.match(clean_id):
            return None

        with self._lock:
            if clean_id in self._diagnostics_by_id:
                return self._diagnostics_by_id[clean_id]

            if self._state_store is not None:
                item = self._state_store.get(f"historical_review_diagnostics:{clean_id}", None)
                if isinstance(item, dict):
                    rec = AuditDiagnosticRecord.from_dict(item)
                    if rec not in self._diagnostics:
                        self._diagnostics.append(rec)
                    self._diagnostics_by_id[rec.diagnostic_id] = rec
                    return rec

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
                stored_hist = self._state_store.get("historical_review_diagnostics:history", [])
                if isinstance(stored_hist, list):
                    for item in stored_hist:
                        if isinstance(item, dict):
                            try:
                                r = AuditDiagnosticRecord.from_dict(item)
                                if r not in self._diagnostics:
                                    self._diagnostics.append(r)
                                self._diagnostics_by_id[r.diagnostic_id] = r
                            except Exception:
                                pass

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
            if clean_id in self._alerts_by_id:
                return self._alerts_by_id[clean_id]

            if self._state_store is not None:
                item = self._state_store.get(f"historical_review_alerts:{clean_id}", None)
                if isinstance(item, dict):
                    alert = AuditAlert.from_dict(item)
                    if alert not in self._alerts:
                        self._alerts.append(alert)
                    self._alerts_by_id[alert.alert_id] = alert
                    return alert

            return None

    def list_alerts(
        self,
        investigation_id: Optional[str] = None,
        category: Optional[Union[AuditDiagnosticCategory, str]] = None,
        severity: Optional[Union[AuditDiagnosticSeverity, str]] = None,
        active_only: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> List[AuditAlert]:
        """Query operational alerts with optional filters and pagination."""
        clamped_limit = max(1, min(100, limit))
        clamped_offset = max(0, offset)

        cat_val = category.value if isinstance(category, Enum) else (str(category) if category else None)
        sev_val = severity.value if isinstance(severity, Enum) else (str(severity) if severity else None)

        with self._lock:
            # Hydrate from StateStore if in-memory queue is empty
            if not self._alerts and self._state_store is not None:
                stored_hist = self._state_store.get("historical_review_alerts:history", [])
                if isinstance(stored_hist, list):
                    for item in stored_hist:
                        if isinstance(item, dict):
                            try:
                                a = AuditAlert.from_dict(item)
                                if a not in self._alerts:
                                    self._alerts.append(a)
                                self._alerts_by_id[a.alert_id] = a
                            except Exception:
                                pass

            results = list(self._alerts)

            if investigation_id:
                clean_inv = investigation_id.strip()
                results = [a for a in results if a.investigation_id == clean_inv]

            if cat_val:
                results = [a for a in results if a.category.value == cat_val]

            if sev_val:
                results = [a for a in results if a.severity.value == sev_val]

            if active_only:
                results = [a for a in results if a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE)]

            results.sort(key=lambda a: a.timestamp, reverse=True)
            return results[clamped_offset : clamped_offset + clamped_limit]

    def get_diagnostic_summary(self, investigation_id: str) -> AuditDiagnosticSummary:
        """Generate a roll-up diagnostic summary for a specific investigation."""
        clean_inv = validate_safe_id(investigation_id, "investigation_id")

        with self._lock:
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
                if a.status in (AuditAlertStatus.TRIGGERED, AuditAlertStatus.ACTIVE)
            ]

            latest_ts = matching_diags[0].timestamp if matching_diags else None
            latest_iso = matching_diags[0].timestamp_iso if matching_diags else None

            recovery = any(d.category == AuditDiagnosticCategory.RECOVERY_OBSERVED for d in matching_diags)
            has_active = bool(self._active_failures.get(clean_inv)) or bool(active_alerts and not recovery)
            recent_reasons = tuple(a.reason for a in matching_alerts[:5])

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
            )

    def clear_history(self) -> None:
        """Clear in-memory diagnostics and alerts history (test utility)."""
        with self._lock:
            self._diagnostics.clear()
            self._diagnostics_by_id.clear()
            self._alerts.clear()
            self._alerts_by_id.clear()
            self._last_alert_time.clear()
            self._suppressed_counts.clear()
            self._active_failures.clear()
