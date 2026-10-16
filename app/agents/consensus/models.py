"""KAISER — Deterministic Multi-Agent Specialist Consensus Models.

Defines the typed consensus contract, agent findings container, and immutable
consensus report representing synthesis between specialist intelligence agents.

SAFETY & ARCHITECTURAL INVARIANTS:
1. Analytical only: Zero trade recommendations, order placement, or execution fields.
2. Evidence-grounded: Agreement or disagreement is derived from explicit agent findings.
3. Strict 5-state consensus contract:
   - AGREEMENT
   - DISAGREEMENT
   - INSUFFICIENT_EVIDENCE
   - PARTIAL_RESULT
   - FAILED
4. Determinism: Identical agent findings yield identical consensus outcomes.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from app.analysis.opportunities.models import validate_no_prohibited_fields
from app.core.exceptions import ValidationError


class ConsensusStatus(str, Enum):
    """Categorical outcome of deterministic multi-agent consensus evaluation."""

    AGREEMENT = "AGREEMENT"
    DISAGREEMENT = "DISAGREEMENT"
    INSUFFICIENT_EVIDENCE = "INSUFFICIENT_EVIDENCE"
    PARTIAL_RESULT = "PARTIAL_RESULT"
    FAILED = "FAILED"


class AgentFinding:
    """Standardized finding record extracted from a participating specialist agent."""

    def __init__(
        self,
        agent_name: str,
        agent_type: str,
        posture: str,
        confidence: str = "NONE",
        summary: str = "",
        facts: Optional[Dict[str, Any]] = None,
        limitations: Optional[Sequence[str]] = None,
        is_valid: bool = True,
        error: Optional[str] = None,
    ) -> None:
        self.agent_name = str(agent_name)
        self.agent_type = str(agent_type)
        self.posture = str(posture).strip().upper()
        self.confidence = str(confidence).strip().upper()
        self.summary = str(summary)
        self.facts = copy.deepcopy(facts or {})
        self.limitations = tuple(str(l) for l in (limitations or ()))
        self.is_valid = bool(is_valid)
        self.error = str(error) if error is not None else None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "agent_type": self.agent_type,
            "posture": self.posture,
            "confidence": self.confidence,
            "summary": self.summary,
            "facts": copy.deepcopy(self.facts),
            "limitations": list(self.limitations),
            "is_valid": self.is_valid,
            "error": self.error,
        }


def compute_deterministic_consensus_id(
    symbol: str,
    timestamp: float,
    findings: Mapping[str, AgentFinding],
) -> str:
    """Generate a deterministic, tamper-evident consensus identifier."""
    clean_sym = symbol.strip().upper()
    ts_int = int(timestamp * 1000)
    sorted_agents = sorted(findings.keys())
    payload = {
        "symbol": clean_sym,
        "timestamp_ms": ts_int,
        "agents": [
            {
                "name": name,
                "type": findings[name].agent_type,
                "posture": findings[name].posture,
                "is_valid": findings[name].is_valid,
            }
            for name in sorted_agents
        ],
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"cons_{clean_sym.lower()}_{ts_int}_{digest}"


class AgentConsensusReport:
    """Immutable multi-agent consensus report evaluating specialist alignment."""

    def __init__(
        self,
        consensus_id: str,
        symbol: str,
        timestamp: float,
        iso_timestamp: str,
        status: ConsensusStatus,
        synthesized_posture: Optional[str],
        participating_agents: Sequence[str],
        agent_findings: Mapping[str, AgentFinding],
        agreement_points: Sequence[str],
        disagreement_points: Sequence[str],
        limitations: Sequence[str],
        voice_summary: str,
        provenance: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.consensus_id = str(consensus_id)
        self.symbol = symbol.strip().upper()
        self.timestamp = float(timestamp)
        self.iso_timestamp = str(iso_timestamp)
        self.status = status
        self.synthesized_posture = str(synthesized_posture).strip().upper() if synthesized_posture else None
        self.participating_agents = tuple(str(a) for a in participating_agents)
        self.agent_findings = copy.deepcopy(dict(agent_findings))
        self.agreement_points = tuple(str(p) for p in agreement_points)
        self.disagreement_points = tuple(str(p) for p in disagreement_points)
        self.limitations = tuple(str(l) for l in limitations)
        self.voice_summary = str(voice_summary)
        self.provenance = copy.deepcopy(provenance or {})

        # Safety invariant: strictly prohibit trading recommendations
        validate_no_prohibited_fields(self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "consensus_id": self.consensus_id,
            "symbol": self.symbol,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
            "status": self.status.value,
            "synthesized_posture": self.synthesized_posture,
            "participating_agents": list(self.participating_agents),
            "agent_findings": {k: v.to_dict() for k, v in self.agent_findings.items()},
            "agreement_points": list(self.agreement_points),
            "disagreement_points": list(self.disagreement_points),
            "limitations": list(self.limitations),
            "voice_summary": self.voice_summary,
            "provenance": copy.deepcopy(self.provenance),
        }
