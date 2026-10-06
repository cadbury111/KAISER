# KAISER — PHASE 2, STEP 29 IMPLEMENTATION REPORT
**INVESTIGATION DISCREPANCY RESOLUTION & EVIDENCE RECONCILIATION FOUNDATION**

> **Core Architectural Statement:**  
> *"This discrepancy resolution layer provides a deterministic, auditable foundation for human-driven evidence reconciliation only. It does not automatically decide which investigation is correct, rank strategies, predict profitability, or provide execution recommendations."*

---

## 1. Objective

Phase 2 Step 29 establishes a deterministic, auditable foundation for **human-driven discrepancy resolution and evidence reconciliation** on top of:
- **Phase 2 Step 22:** Experiment Comparison (`RunComparisonEngine`)
- **Phase 2 Step 23:** Evidence Synthesis (`EvidenceSynthesisReport`, `EvidenceSynthesisEngine`)
- **Phase 2 Step 24:** Opportunity Assessment (`OpportunityCandidate`, `OpportunityAssessmentEngine`)
- **Phase 2 Step 25:** Opportunity Prioritization & Investigation Queue (`OpportunityPriorityRecord`, `InvestigationQueue`)
- **Phase 2 Step 26:** Human Investigation Sessions (`InvestigationSessionManager`, `InvestigationSession`)
- **Phase 2 Step 27:** Investigation Reporting (`InvestigationReport`, `InvestigationReportingService`)
- **Phase 2 Step 28:** Cross-Investigation Comparison & Discrepancy Detection (`InvestigationComparisonEngine`, `InvestigationDiscrepancy`)

While Step 28 identifies analytical discrepancies between reports, **Step 29 manages the human-in-the-loop lifecycle** of those discrepancies:
1. What discrepancy was detected?
2. What evidence items are being reconciled?
3. What human-reviewed resolution was recorded?
4. Why was that resolution recorded?
5. What source evidence and provenance support it?
6. Does the discrepancy remain unresolved, deferred, rejected, or resolved?

The system **never automatically selects a winning report or auto-resolves discrepancies**. It strictly records and audits human decisions.

---

## 2. Architecture & Lineage

The resolution layer maintains unbroken backwards lineage:

```
DiscrepancyResolutionCase (Phase 2 Step 29)
      ↓ (binds detected discrepancy)
InvestigationDiscrepancy (Phase 2 Step 28)
      ↓
InvestigationComparison (Phase 2 Step 28)
      ↓
InvestigationReport (Phase 2 Step 27)
      ↓
InvestigationSession (Phase 2 Step 26)
      ↓
OpportunityPriorityRecord (Phase 2 Step 25)
      ↓
OpportunityCandidate (Phase 2 Step 24)
      ↓
EvidenceSynthesisReport (Phase 2 Step 23)
      ↓
Analysis Runs / Data Provenance / Data Quality (Steps 19-21)
```

No analytical context or provenance is lost. Every resolution case preserves the comparison ID, discrepancy ID, report IDs, evidence references, reviewer identifier, rationale, and audit trail.

---

## 3. Core Domain Models

All domain models are defined as immutable/frozen dataclasses in [`app/analysis/opportunities/resolution/models.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py):

| Model | Purpose | Immutability |
|---|---|---|
| [`DiscrepancyResolutionCase`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Central immutable case model binding a discrepancy to human reviews and audits | `@dataclass(frozen=True)` |
| [`ResolutionState`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Explicit 8-state machine enum for resolution lifecycle | `str, Enum` |
| [`EvidenceReconciliationRecord`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Record of human-reviewed reconciliation between conflicting evidence items | `@dataclass(frozen=True)` |
| [`ResolutionDecision`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Audit record of a human decision tagged as `HUMAN_REVIEW_DECISION` | `@dataclass(frozen=True)` |
| [`ResolutionAuditRecord`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Deterministic audit record tracking previous/new state, actor, reason, timestamp | `@dataclass(frozen=True)` |
| [`ResolutionVoiceSummary`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Speech-ready, non-directional summary for the voice interface | `@dataclass(frozen=True)` |

### Explicit Claim Distinction
To preserve epistemic rigor, decisions are explicitly typed under `DecisionClaimType`:
- `FACT`: Empirical statements verified by underlying data/metrics.
- `INTERPRETATION`: Analyst hypotheses or subjective assessments.
- `HUMAN_REVIEW_DECISION`: The formal human resolution choice.

A human decision is **never presented as an automatically derived fact**.

---

## 4. State Machine & Transitions

The lifecycle of a discrepancy resolution case follows a deterministic transition matrix:

```
                  ┌───────────────┐
                  │     OPEN      │
                  └───────┬───────┘
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
    ┌──────────────┐┌──────────────┐┌──────────────┐
    │ UNDER_REVIEW ││EVIDENCE_REQ. ││   DEFERRED   │
    └───────┬──────┘└──────┬───────┘└──────┬───────┘
            │              │               │
            ▼              ▼               ▼
      ┌───────────────────────────┐   (Reopenable)
      │  RECONCILIATION_RECORDED  │
      └─────────────┬─────────────┘
            ┌───────┴───────┐
            ▼               ▼
     ┌─────────────┐ ┌─────────────┐   ┌─────────────┐
     │  RESOLVED   │ │  REJECTED   │   │ INVALIDATED │
     │ (Terminal)  │ │ (Terminal)  │   │ (Terminal)  │
     └─────────────┘ └─────────────┘   └─────────────┘
```

### Transition Invariants
- `TERMINAL_RESOLUTION_STATES = {RESOLVED, REJECTED, INVALIDATED}`.
- Terminal cases are strictly immutable and cannot be transitioned or modified.
- Arbitrary skips (e.g. `OPEN` direct to `RESOLVED` without review) are rejected with `ValidationError`.

---

## 5. Evidence Reconciliation Rules

The system provides deterministic classifications under [`EvidenceRelationshipType`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py):
- `SAME_EVIDENCE`
- `SUPPORTING_EVIDENCE`
- `CONTRADICTORY_EVIDENCE`
- `COMPLEMENTARY_EVIDENCE`
- `OUTDATED_EVIDENCE`
- `DIFFERENT_SCOPE`
- `DIFFERENT_DATA_QUALITY`
- `DIFFERENT_METHODOLOGY`
- `INSUFFICIENT_EVIDENCE`
- `UNAVAILABLE_EVIDENCE`

Reconciliation outcomes are recorded under [`ReconciliationOutcome`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py):
- `BOTH_PRESERVED_VALID`
- `ONE_SUPERSEDES_OTHER`
- `DIFFERING_CONTEXT_EXPLAINED`
- `METHODOLOGICAL_DIFFERENCE_ACKNOWLEDGED`
- `QUALITY_DEGRADATION_CONFIRMED`
- `INSUFFICIENT_DATA_FOR_SYNTHESIS`
- `EVIDENCE_INVALIDATED`

**Invariant for Conflicting Evidence:**
Conflicting evidence is never overwritten or merged into a fabricated single truth. Both sources, their timestamps, their quality scores, and their provenance are preserved in the record alongside the human reconciliation rationale.

---

## 6. Service Layer (`DiscrepancyResolutionService`)

Defined in [`app/analysis/opportunities/resolution/service.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/service.py):
- **Thread Safety:** Thread-safe in-memory cache guarded by `threading.RLock()`.
- **Bounded StateStore Integration:** Ring-buffer storage with bounded capacity (max 100 cases).
- **Human Boundary Enforcement:** Rejects resolution attempts where `reviewer_id` is `"ai"`, `"autonomous"`, `"system"`, or `"bot"`.
- **EventBus Integration:** Emits sanitized lifecycle events.
- **Methods:**
  - `create_resolution_case(...)`
  - `get_case(...)`
  - `list_cases(...)`
  - `transition_case(...)`
  - `attach_evidence(...)`
  - `record_reconciliation(...)`
  - `record_human_resolution(...)`
  - `defer_resolution(...)`
  - `invalidate_resolution(...)`
  - `get_voice_summary(...)`

---

## 7. EventBus & StateStore Integration

### Lifecycle Events (`app/core/events/event.py`)
- `DISCREPANCY_RESOLUTION_CASE_CREATED`
- `DISCREPANCY_RESOLUTION_STATE_CHANGED`
- `EVIDENCE_ATTACHED_TO_RESOLUTION`
- `EVIDENCE_RECONCILIATION_RECORDED`
- `HUMAN_RESOLUTION_RECORDED`
- `DISCREPANCY_RESOLUTION_DEFERRED`
- `DISCREPANCY_RESOLUTION_INVALIDATED`
- `DISCREPANCY_RESOLUTION_FAILED`

### StateStore Keys
- `discrepancy_resolution:latest`
- `discrepancy_resolution:<case_id>`
- `discrepancy_resolution:by_discrepancy:<discrepancy_id>`
- `discrepancy_resolution:history` (bounded ring buffer)

---

## 8. Command Handlers (`MasterOrchestrator`)

Registered in [`app/analysis/opportunities/resolution/handlers.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/handlers.py):
1. `create_discrepancy_resolution_case`
2. `get_discrepancy_resolution_case`
3. `list_discrepancy_resolution_cases`
4. `transition_discrepancy_resolution`
5. `attach_resolution_evidence`
6. `record_evidence_reconciliation`
7. `record_human_resolution`
8. `defer_discrepancy_resolution`
9. `invalidate_discrepancy_resolution`

All handlers reject prohibited execution fields and sanitize outputs.

---

## 9. Dashboard API Endpoints

Loopback-only read-only endpoints in [`app/portfolio/dashboard/server.py`](file:///c:/Users/user/Desktop/KAISER/app/portfolio/dashboard/server.py):
- `GET /api/discrepancy-resolutions` — Query and list resolution cases.
- `GET /api/discrepancy-resolutions/{id}` — Retrieve case details and decision.
- `GET /api/discrepancy-resolutions/{id}/evidence` — Retrieve attached and reconciled evidence.
- `GET /api/discrepancy-resolutions/{id}/audit` — Retrieve deterministic lifecycle audit trail.
- `HEAD` supported on all routes.
- Mutating requests (`POST`, `PUT`, `DELETE`) return `405 Method Not Allowed`.
- Path traversal and null-byte injection return `400 Bad Request`.

---

## 10. Voice Summary

Voice-safe summaries structured deterministically:
> *"Discrepancy disc_test_01 remains unresolved in OPEN state. 2 evidence items attached. Human review is required."*  
> *"Discrepancy disc_test_01 has been resolved by human review. 1 evidence reconciliations recorded."*

**Invariant:** Strictly forbids trading terms (`BUY`, `SELL`, `HOLD`, `LONG`, `SHORT`, `ORDER`, `EXECUTE`, `PROFIT`).

---

## 11. Security Controls & Numerical Integrity

- **Prohibited Field Rejection:** Recursively checks against `action`, `side`, `order`, `quantity`, `leverage`, `stop_loss`, `take_profit`, `expected_profit`, `trade_ranking`, etc.
- **Numerical Integrity:** Rejects `NaN`, `+Infinity`, `-Infinity`; preserves `0` and `0.0`.
- **Identifier Validation:** Enforces `^[a-zA-Z0-9_\-\.:]+$` pattern.

---

## 12. Verification & Testing

### A. Focused Test Suite
`tests/test_phase_2_step_29_investigation_resolution.py` covering 25 comprehensive test cases:
1. `test_domain_model_validation` — Field constraints and serialization.
2. `test_immutability` — Frozen dataclass immutability.
3. `test_valid_state_transitions` — State machine flow.
4. `test_invalid_state_transitions` — Rejection of invalid skips.
5. `test_terminal_state_immutability` — Immutability of terminal cases.
6. `test_evidence_attachment` — Supporting evidence ID association.
7. `test_evidence_reconciliation` — Recording reconciliation without overwriting conflict.
8. `test_human_resolution_recording` — Human decision tagging.
9. `test_reject_autonomous_resolution` — Rejection of non-human resolution attempts.
10. `test_invalid_discrepancy_handling` — Empty/malformed discrepancy ID rejection.
11. `test_invalid_comparison_handling` — Empty/malformed comparison ID rejection.
12. `test_provenance_linkage` — Lineage and provenance IDs in reconciliation.
13. `test_deterministic_ids` — Deterministic SHA-256 ID generators.
14. `test_event_emission` — Lifecycle EventBus events.
15. `test_statestore_integration` — Bounded StateStore retention.
16. `test_handler_behavior` — MasterOrchestrator command dispatch.
17. `test_dashboard_get` — Read-only loopback GET endpoints.
18. `test_dashboard_head` — HEAD request verification.
19. `test_dashboard_mutation_rejection` — 405 Method Not Allowed on POST.
20. `test_path_traversal_rejection` — 400 Bad Request on path traversal.
21. `test_secret_sanitization_and_prohibited_fields` — Rejection of trade keywords.
22. `test_non_finite_number_rejection` — Rejection of NaN and Infinity.
23. `test_bounded_collection_behavior` — History pruning at capacity.
24. `test_concurrent_access` — Multi-threaded concurrent evidence attachment.
25. `test_execution_safety_invariants` — Execution locked, voice safety, JUJU isolation.

**Result: 25 passed in 2.60s.**

### B. Full Regression Suite
Ran `python -m pytest -q` across the entire repository:
**Result: 1206 passed in 79.50s (0:01:19). Zero failures.**

### C. System Startup & Dashboard Verification
- `python run.py`: Exits with code 0.
- Registered all 9 discrepancy resolution handlers with MasterOrchestrator.
- Logged: `Investigation discrepancy resolution service: ready (max_history=100)`.
- Verified `Settings.execution_locked = True`.
- Verified Trading Engine: `not initialized`.
- Verified JUJU: Completely untouched.

---

## 13. Deliverable Files

| File | Status | Description |
|---|---|---|
| [`app/analysis/opportunities/resolution/__init__.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/__init__.py) | Created | Package export for resolution models, service, and handlers |
| [`app/analysis/opportunities/resolution/models.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/models.py) | Created | Immutable domain models, state machine, reconciliation records, decisions, audits |
| [`app/analysis/opportunities/resolution/service.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/service.py) | Created | `DiscrepancyResolutionService` managing cases, state transitions, StateStore, EventBus |
| [`app/analysis/opportunities/resolution/handlers.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/resolution/handlers.py) | Created | 9 MasterOrchestrator command handlers for discrepancy resolution workflows |
| [`app/core/events/event.py`](file:///c:/Users/user/Desktop/KAISER/app/core/events/event.py) | Modified | Added 8 discrepancy resolution lifecycle event types to `EventType` |
| [`app/portfolio/dashboard/server.py`](file:///c:/Users/user/Desktop/KAISER/app/portfolio/dashboard/server.py) | Modified | Added endpoint 22 for discrepancy resolutions, sub-resources, 405 on mutations |
| [`app/bootstrap.py`](file:///c:/Users/user/Desktop/KAISER/app/bootstrap.py) | Modified | Initialized `DiscrepancyResolutionService`, registered handlers, wired dashboard |
| [`app/analysis/opportunities/__init__.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/__init__.py) | Modified | Exported resolution models and service |
| [`app/analysis/__init__.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/__init__.py) | Modified | Exported resolution models and service |
| [`tests/test_phase_2_step_29_investigation_resolution.py`](file:///c:/Users/user/Desktop/KAISER/tests/test_phase_2_step_29_investigation_resolution.py) | Created | 25 unit, integration, property, security, concurrency, and safety tests |
| [`KAISER_PHASE_2_STEP_29_IMPLEMENTATION_REPORT.md`](file:///c:/Users/user/Desktop/KAISER/KAISER_PHASE_2_STEP_29_IMPLEMENTATION_REPORT.md) | Created | Comprehensive implementation report |

---

## 14. Exact Git Commit Hash

- **Git Commit Hash:** `2b01555faf9474a6a9a4cf16a09f0d92c91735fd`
