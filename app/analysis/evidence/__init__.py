"""KAISER — Evidence Synthesis & Decision-Support Reporting Foundation.

Exports domain models, status classifications, conflict and uncertainty types,
the deterministic synthesis engine, and orchestrator handlers.
"""

from __future__ import annotations

from app.analysis.evidence.engine import (
    DEFAULT_MAX_SYNTHESIS_HISTORY,
    EvidenceSynthesisEngine,
)
from app.analysis.evidence.handlers import (
    register_evidence_handlers,
)
from app.analysis.evidence.models import (
    DEFAULT_MAX_CONFLICTS,
    DEFAULT_MAX_EVIDENCE_ITEMS,
    DEFAULT_MAX_LIMITATIONS,
    DEFAULT_MAX_OBSERVATIONS,
    DEFAULT_MAX_SOURCE_RUNS,
    ConflictResolutionStatus,
    ConflictSeverity,
    ConflictType,
    EvidenceConflict,
    EvidenceItem,
    EvidenceStatus,
    EvidenceSynthesisReport,
    EvidenceType,
    EvidenceUncertainty,
    EvidenceVoiceSummary,
    KeyObservation,
    MissingEvidence,
    ObservationType,
    SampleCoverage,
    UncertaintySource,
    evidence_priority_key,
)

__all__ = [
    "ConflictResolutionStatus",
    "ConflictSeverity",
    "ConflictType",
    "DEFAULT_MAX_CONFLICTS",
    "DEFAULT_MAX_EVIDENCE_ITEMS",
    "DEFAULT_MAX_LIMITATIONS",
    "DEFAULT_MAX_OBSERVATIONS",
    "DEFAULT_MAX_SOURCE_RUNS",
    "DEFAULT_MAX_SYNTHESIS_HISTORY",
    "EvidenceConflict",
    "EvidenceItem",
    "EvidenceStatus",
    "EvidenceSynthesisEngine",
    "EvidenceSynthesisReport",
    "EvidenceType",
    "EvidenceUncertainty",
    "EvidenceVoiceSummary",
    "KeyObservation",
    "MissingEvidence",
    "ObservationType",
    "SampleCoverage",
    "UncertaintySource",
    "evidence_priority_key",
    "register_evidence_handlers",
]
