"""KAISER — Consolidated Investigation Review Evolution & Historical Consistency Package.

Phase 2, Step 32: Deterministic historical tracking layer for recording how investigation
evidence, discrepancy resolution, review coverage, and analytical consistency change over time.
"""

from app.analysis.opportunities.evolution.handlers import (
    register_evolution_handlers,
)
from app.analysis.opportunities.evolution.models import (
    ChangeClassification,
    ConflictLifecycleState,
    DataQualityEvolutionState,
    EvolutionDimensionName,
    EvolutionDimensionRecord,
    HistoricalReviewSnapshot,
    HistoricalSnapshotLifecycleState,
    RecurringReviewConflict,
    ReproducibilityEvolutionState,
    ReviewCoverageEvolutionState,
    ReviewEvolutionRecord,
    ReviewEvolutionVoiceSummary,
    TERMINAL_SNAPSHOT_LIFECYCLE_STATES,
    compute_deterministic_evolution_id,
    compute_deterministic_snapshot_id,
    validate_no_prohibited_fields,
    validate_numerical_integrity,
)
from app.analysis.opportunities.evolution.service import (
    HistoricalReviewEvolutionService,
)

__all__ = [
    "ChangeClassification",
    "ConflictLifecycleState",
    "DataQualityEvolutionState",
    "EvolutionDimensionName",
    "EvolutionDimensionRecord",
    "HistoricalReviewSnapshot",
    "HistoricalSnapshotLifecycleState",
    "HistoricalReviewEvolutionService",
    "RecurringReviewConflict",
    "ReproducibilityEvolutionState",
    "ReviewCoverageEvolutionState",
    "ReviewEvolutionRecord",
    "ReviewEvolutionVoiceSummary",
    "TERMINAL_SNAPSHOT_LIFECYCLE_STATES",
    "compute_deterministic_evolution_id",
    "compute_deterministic_snapshot_id",
    "register_evolution_handlers",
    "validate_no_prohibited_fields",
    "validate_numerical_integrity",
]
