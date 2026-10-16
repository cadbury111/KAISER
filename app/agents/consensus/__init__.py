"""KAISER — Multi-Agent Specialist Coordination & Deterministic Consensus Package.

Phase 2, Step 49: Framework for coordinating specialist intelligence agents,
evaluating evidence alignment under a deterministic consensus contract,
and communicating analytical agreement or divergence.
"""

from __future__ import annotations

from app.agents.consensus.handlers import register_consensus_handlers
from app.agents.consensus.models import (
    AgentConsensusReport,
    AgentFinding,
    ConsensusStatus,
    compute_deterministic_consensus_id,
)
from app.agents.consensus.service import (
    DEFAULT_MAX_CONSENSUS_REPORTS,
    MultiAgentCoordinationService,
)

__all__ = [
    "ConsensusStatus",
    "AgentFinding",
    "AgentConsensusReport",
    "compute_deterministic_consensus_id",
    "MultiAgentCoordinationService",
    "register_consensus_handlers",
    "DEFAULT_MAX_CONSENSUS_REPORTS",
]
