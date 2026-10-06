"""KAISER — Investigation Discrepancy Resolution & Evidence Reconciliation Package.

Phase 2, Step 29: Deterministic human-driven discrepancy resolution,
evidence reconciliation records, state machine transitions, and audit logs.
"""

from app.analysis.opportunities.resolution.handlers import register_discrepancy_resolution_handlers
from app.analysis.opportunities.resolution.models import (
    DEFAULT_MAX_RESOLUTION_HISTORY,
    PROHIBITED_RECOMMENDATION_FIELDS,
    SAFE_ID_PATTERN,
    TERMINAL_RESOLUTION_STATES,
    VALID_RESOLUTION_TRANSITIONS,
    DecisionClaimType,
    DiscrepancyResolutionCase,
    EvidenceReconciliationRecord,
    EvidenceRelationshipType,
    ReconciliationOutcome,
    ResolutionAuditRecord,
    ResolutionDecision,
    ResolutionState,
    ResolutionVoiceSummary,
    compute_deterministic_audit_id,
    compute_deterministic_case_id,
    compute_deterministic_decision_id,
    compute_deterministic_reconciliation_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.resolution.service import DiscrepancyResolutionService

__all__ = [
    "DEFAULT_MAX_RESOLUTION_HISTORY",
    "PROHIBITED_RECOMMENDATION_FIELDS",
    "SAFE_ID_PATTERN",
    "TERMINAL_RESOLUTION_STATES",
    "VALID_RESOLUTION_TRANSITIONS",
    "DecisionClaimType",
    "DiscrepancyResolutionCase",
    "EvidenceReconciliationRecord",
    "EvidenceRelationshipType",
    "ReconciliationOutcome",
    "ResolutionAuditRecord",
    "ResolutionDecision",
    "ResolutionState",
    "ResolutionVoiceSummary",
    "compute_deterministic_audit_id",
    "compute_deterministic_case_id",
    "compute_deterministic_decision_id",
    "compute_deterministic_reconciliation_id",
    "validate_no_prohibited_fields",
    "validate_numerical_integrity",
    "DiscrepancyResolutionService",
    "register_discrepancy_resolution_handlers",
]
