"""KAISER — Human Investigation Session Orchestrator Command Handlers.

Provides MasterOrchestrator command handlers for human investigation sessions:
- `create_investigation_session`: Initialize an investigation session for a candidate.
- `start_investigation_session`: Transition session to ACTIVE.
- `pause_investigation_session`: Temporarily pause active session.
- `resume_investigation_session`: Resume paused or evidence-waiting session.
- `add_investigation_observation`: Record analyst observation (fact or interpretation).
- `review_investigation_evidence`: Record evaluation on specific evidence item.
- `add_investigation_uncertainty`: Document epistemic uncertainty.
- `add_investigation_limitation`: Document analytical constraint/limitation.
- `add_investigation_review_note`: Record structured review commentary.
- `set_investigation_conclusion`: Set non-prescriptive analytical conclusion.
- `request_more_investigation_evidence`: Transition to REQUIRES_MORE_EVIDENCE.
- `complete_investigation_session`: Conclude and freeze session with conclusion.
- `invalidate_investigation_session`: Mark session invalidated due to data/integrity issue.
- `cancel_investigation_session`: Cancel session.
- `reopen_investigation_session`: Open linked follow-up revision session.
- `get_investigation_session`: Retrieve session details.
- `list_investigation_sessions`: Query sessions with pagination and filters.
- `get_investigation_audit_trail`: Retrieve immutable audit events.

SAFETY INVARIANTS:
1. Rejects any prohibited execution, trading, or recommendation parameters.
2. Strictly read-only decision support; execution engine remains locked.
3. Redacts secrets and sanitizes errors before responding.
"""

from __future__ import annotations

from collections.abc import Mapping
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from app.analysis.opportunities.models import validate_no_prohibited_fields
from app.analysis.opportunities.investigation.session import InvestigationSessionManager
from app.core.exceptions import ValidationError
from app.orchestrator.models import OrchestratorRequest

if TYPE_CHECKING:
    from app.orchestrator.master import MasterOrchestrator

logger = logging.getLogger("kaiser.analysis.opportunities.investigation.handlers")

SAFE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-\.]+$")


def register_investigation_handlers(
    orchestrator: MasterOrchestrator,
    investigation_manager: InvestigationSessionManager,
) -> None:
    """Register investigation session commands with MasterOrchestrator."""

    def _extract_params(req: OrchestratorRequest) -> Dict[str, Any]:
        """Extract and validate parameters from OrchestratorRequest."""
        p = dict(
            getattr(req, "context", {})
            or getattr(req, "parameters", {})
            or getattr(req, "payload", {})
            or {}
        )
        validate_no_prohibited_fields(p)
        return p

    def handle_create_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'create_investigation_session' command."""
        p = _extract_params(req)
        candidate = p.get("candidate")
        if not candidate:
            return {
                "status": "invalid_request",
                "message": "Missing 'candidate' parameter.",
                "read_only": True,
            }

        priority_rec = p.get("priority_record")
        reviewer_id = p.get("reviewer_id")
        parent_id = p.get("parent_session_id")
        metadata = p.get("metadata", {})

        session = investigation_manager.create_session(
            candidate=candidate,
            priority_record=priority_rec,
            reviewer_id=reviewer_id,
            parent_session_id=parent_id,
            metadata=metadata,
        )

        return {
            "status": "success",
            "session_id": session.session_id,
            "candidate_id": session.candidate_id,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_start_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'start_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reviewer_id = p.get("reviewer_id")

        session = investigation_manager.start_session(session_id, reviewer_id=reviewer_id)
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_pause_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'pause_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reviewer_id = p.get("reviewer_id")
        reason = str(p.get("reason", ""))

        session = investigation_manager.pause_session(session_id, reviewer_id=reviewer_id, reason=reason)
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_resume_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'resume_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reviewer_id = p.get("reviewer_id")

        session = investigation_manager.resume_session(session_id, reviewer_id=reviewer_id)
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_add_investigation_observation(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'add_investigation_observation' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        obs_type = p.get("observation_type", "INTERPRETATION")
        statement = str(p.get("statement", ""))
        evidence_refs = p.get("evidence_refs", [])
        reviewer_id = p.get("reviewer_id")

        obs = investigation_manager.add_observation(
            session_id=session_id,
            observation_type=obs_type,
            statement=statement,
            evidence_refs=evidence_refs,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session_id,
            "observation_id": obs.observation_id,
            "observation": obs.to_dict(),
            "read_only": True,
        }

    def handle_review_investigation_evidence(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'review_investigation_evidence' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        evidence_id = str(p.get("evidence_id", "")).strip()
        disposition = p.get("disposition", "REVIEWED")
        notes = str(p.get("notes", ""))
        reviewer_id = p.get("reviewer_id")

        record = investigation_manager.review_evidence(
            session_id=session_id,
            evidence_id=evidence_id,
            disposition=disposition,
            notes=notes,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session_id,
            "review_id": record.review_id,
            "review": record.to_dict(),
            "read_only": True,
        }

    def handle_add_investigation_uncertainty(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'add_investigation_uncertainty' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        description = str(p.get("description", ""))
        severity = str(p.get("severity", "MEDIUM"))
        evidence_refs = p.get("evidence_refs", [])
        reviewer_id = p.get("reviewer_id")

        unc = investigation_manager.add_uncertainty(
            session_id=session_id,
            description=description,
            severity=severity,
            evidence_refs=evidence_refs,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session_id,
            "uncertainty_id": unc.uncertainty_id,
            "uncertainty": unc.to_dict(),
            "read_only": True,
        }

    def handle_add_investigation_limitation(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'add_investigation_limitation' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        description = str(p.get("description", ""))
        category = str(p.get("category", "ANALYTICAL_SCOPE"))
        reviewer_id = p.get("reviewer_id")

        lim = investigation_manager.add_limitation(
            session_id=session_id,
            description=description,
            category=category,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session_id,
            "limitation_id": lim.limitation_id,
            "limitation": lim.to_dict(),
            "read_only": True,
        }

    def handle_add_investigation_review_note(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'add_investigation_review_note' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        content = str(p.get("content", ""))
        reviewer_id = p.get("reviewer_id")

        note = investigation_manager.add_review_note(
            session_id=session_id,
            content=content,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session_id,
            "note_id": note.note_id,
            "note": note.to_dict(),
            "read_only": True,
        }

    def handle_set_investigation_conclusion(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'set_investigation_conclusion' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        conclusion_state = p.get("conclusion_state", "INSUFFICIENT_EVIDENCE")
        rationale = str(p.get("rationale", ""))
        reviewer_id = p.get("reviewer_id")

        conc = investigation_manager.set_conclusion(
            session_id=session_id,
            conclusion_state=conclusion_state,
            rationale=rationale,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session_id,
            "conclusion_id": conc.conclusion_id,
            "conclusion": conc.to_dict(),
            "read_only": True,
        }

    def handle_request_more_investigation_evidence(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'request_more_investigation_evidence' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reason = str(p.get("reason", ""))
        reviewer_id = p.get("reviewer_id")

        session = investigation_manager.request_more_evidence(
            session_id=session_id,
            reason=reason,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_complete_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'complete_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reviewer_id = p.get("reviewer_id")
        conclusion_state = p.get("conclusion_state")
        rationale = p.get("rationale")

        session = investigation_manager.complete_session(
            session_id=session_id,
            reviewer_id=reviewer_id,
            conclusion_state=conclusion_state,
            rationale=rationale,
        )
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "voice_summary": session.voice_summary.to_dict() if session.voice_summary else None,
            "read_only": True,
        }

    def handle_invalidate_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'invalidate_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reason = str(p.get("reason", ""))
        reviewer_id = p.get("reviewer_id")

        session = investigation_manager.invalidate_session(
            session_id=session_id,
            reason=reason,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_cancel_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'cancel_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reason = str(p.get("reason", ""))
        reviewer_id = p.get("reviewer_id")

        session = investigation_manager.cancel_session(
            session_id=session_id,
            reason=reason,
            reviewer_id=reviewer_id,
        )
        return {
            "status": "success",
            "session_id": session.session_id,
            "session_status": session.status.value,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_reopen_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'reopen_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        reviewer_id = p.get("reviewer_id")
        reason = str(p.get("reason", ""))

        session = investigation_manager.reopen_session(
            session_id=session_id,
            reviewer_id=reviewer_id,
            reason=reason,
        )
        return {
            "status": "success",
            "session_id": session.session_id,
            "parent_session_id": session.parent_session_id,
            "revision": session.revision,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_get_investigation_session(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_session' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        if not session_id:
            return {"status": "invalid_request", "message": "Missing 'session_id'.", "read_only": True}

        session = investigation_manager.get_session(session_id)
        if not session:
            return {
                "status": "not_found",
                "session_id": session_id,
                "message": f"Investigation session '{session_id}' not found.",
                "read_only": True,
            }

        return {
            "status": "success",
            "session_id": session.session_id,
            "session": session.to_dict(),
            "read_only": True,
        }

    def handle_list_investigation_sessions(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'list_investigation_sessions' command."""
        p = _extract_params(req)
        limit = int(p.get("limit", 50))
        offset = int(p.get("offset", 0))
        status = p.get("status")
        candidate_id = p.get("candidate_id")

        sessions = investigation_manager.list_sessions(
            limit=max(1, min(100, limit)),
            offset=max(0, offset),
            status=status,
            candidate_id=candidate_id,
        )
        return {
            "status": "success",
            "count": len(sessions),
            "sessions": [s.to_dict() for s in sessions],
            "read_only": True,
        }

    def handle_get_investigation_audit_trail(req: OrchestratorRequest) -> Dict[str, Any]:
        """Handler for 'get_investigation_audit_trail' command."""
        p = _extract_params(req)
        session_id = str(p.get("session_id", "")).strip()
        if not session_id:
            return {"status": "invalid_request", "message": "Missing 'session_id'.", "read_only": True}

        audits = investigation_manager.get_audit_trail(session_id)
        return {
            "status": "success",
            "session_id": session_id,
            "count": len(audits),
            "audit_trail": [a.to_dict() for a in audits],
            "read_only": True,
        }

    # Register handlers with MasterOrchestrator under capability 'investigation_session'
    orchestrator.register_handler(
        command="create_investigation_session",
        handler=handle_create_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="start_investigation_session",
        handler=handle_start_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="pause_investigation_session",
        handler=handle_pause_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="resume_investigation_session",
        handler=handle_resume_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="add_investigation_observation",
        handler=handle_add_investigation_observation,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="review_investigation_evidence",
        handler=handle_review_investigation_evidence,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="add_investigation_uncertainty",
        handler=handle_add_investigation_uncertainty,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="add_investigation_limitation",
        handler=handle_add_investigation_limitation,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="add_investigation_review_note",
        handler=handle_add_investigation_review_note,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="set_investigation_conclusion",
        handler=handle_set_investigation_conclusion,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="request_more_investigation_evidence",
        handler=handle_request_more_investigation_evidence,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="complete_investigation_session",
        handler=handle_complete_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="invalidate_investigation_session",
        handler=handle_invalidate_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="cancel_investigation_session",
        handler=handle_cancel_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="reopen_investigation_session",
        handler=handle_reopen_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_session",
        handler=handle_get_investigation_session,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="list_investigation_sessions",
        handler=handle_list_investigation_sessions,
        capability="investigation_session",
        allow_override=True,
    )
    orchestrator.register_handler(
        command="get_investigation_audit_trail",
        handler=handle_get_investigation_audit_trail,
        capability="investigation_session",
        allow_override=True,
    )

    logger.info("Registered investigation session handlers with MasterOrchestrator.")
