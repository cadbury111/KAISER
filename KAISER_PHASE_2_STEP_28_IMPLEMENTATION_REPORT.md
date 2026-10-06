# KAISER — PHASE 2, STEP 28 IMPLEMENTATION REPORT
**CROSS-INVESTIGATION EVIDENCE COMPARISON & REVIEW CONSISTENCY FOUNDATION**

> **Core Architectural Statement:**  
> *"This comparison layer reports analytical differences and review consistency only. It does not rank trades, predict profitability, or provide execution recommendations."*

---

## 1. Objective

Phase 2 Step 28 establishes a deterministic, auditable Cross-Investigation Evidence Comparison layer in KAISER on top of:
- **Phase 2 Step 22:** Analysis Run Comparison (`RunComparisonEngine`)
- **Phase 2 Step 23:** Evidence Synthesis (`EvidenceSynthesisReport`, `EvidenceSynthesisEngine`)
- **Phase 2 Step 24:** Opportunity Assessment (`OpportunityCandidate`, `OpportunityAssessmentEngine`)
- **Phase 2 Step 25:** Opportunity Prioritization & Investigation Queue (`OpportunityPriorityRecord`, `InvestigationQueue`)
- **Phase 2 Step 26:** Human Investigation Session (`InvestigationSessionManager`, `InvestigationSession`)
- **Phase 2 Step 27:** Investigation Reporting (`InvestigationReport`, `InvestigationReportingService`)

Step 28 enables analytical comparison across multiple completed `InvestigationReport` artifacts to identify:
1. **Shared Evidence:** Empirical findings and reviewer dispositions corroborated across investigations.
2. **Differing Evidence:** Evidence present in one report but added, removed, or modified in another.
3. **Conflicting Evidence:** Contradictory reviewer dispositions (e.g., SUPPORTING vs. CONFLICTING) assigned to the same empirical evidence items.
4. **Observation Classification Shifts:** Explicit detection of semantic category changes (e.g., FACT reclassified as INTERPRETATION, or vice-versa).
5. **Uncertainty & Limitation Tracking:** New or resolved analytical uncertainties and varying methodological limitations.
6. **Provenance & Data-Quality Differences:** Lineage shifts, source timestamp variance, completeness ratio changes, and freshness degradation.
7. **Reproducibility Comparison:** Evaluation of reproducibility identifiers, configuration fingerprints, and underlying analysis runs.
8. **Review Consistency & Unresolved Discrepancies:** Systematic structured accounting of discrepancies across reports without judging which reviewer is "correct".

The output is strictly an **analytical comparison**, completely isolated from execution or ranking.

---

## 2. Architecture & Lineage

The comparison layer evaluates completed `InvestigationReport` instances and computes an immutable comparison record preserving full backwards traceability:

```
InvestigationComparison (Phase 2 Step 28)
      ↓ (compares 2+ completed reports)
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

No analytical lineage is lost or flattened. Every comparison carries full report IDs, candidate IDs, provenance comparisons, reproducibility comparisons, and evidence references.

---

## 3. Comparability Dimensions & Deterministic States

Comparability is never silently assumed. It is evaluated across 10 deterministic dimensions:
1. **Symbol Alignment:** Instrument symbol match across reports.
2. **Timeframe Compatibility:** Primary analysis interval match.
3. **Observation Period:** Temporal overlap of underlying evidence windows.
4. **Candidate Lineage:** Candidate hypotheses and parent priorities.
5. **Evidence Schema:** Compatible structural schema version.
6. **Report Version:** Compatible investigation reporting contract.
7. **Provenance Lineage:** Compatible source provenance and lineage records.
8. **Quality Metadata:** Compatible data-quality status indicators.
9. **Reproducibility Metadata:** Compatible reproducibility IDs and fingerprints.
10. **Analytical Context:** Consistent analytical scope and assumptions.

### Comparability States
- `COMPARABLE`: All core dimensions (symbol, timeframe, candidate hypothesis) align.
- `PARTIALLY_COMPARABLE`: Same instrument symbol, but differing timeframe or candidate hypothesis revisions.
- `NOT_COMPARABLE`: Incompatible instrument symbols or non-overlapping fundamental domains.
- `INVALID_INPUT`: One or more reports marked `INVALID` or corrupt.
- `INCOMPLETE_EVIDENCE`: One or more reports marked `INCOMPLETE` or lacking essential analytical conclusions.

---

## 4. Comparison Dimensions & Diff Engines

### A. Evidence Diff
Categorizes evidence items across compared reports into six deterministic categories:
- `UNCHANGED`: Identical evidence presence and consistent reviewer dispositions.
- `ADDED`: Evidence introduced in later or alternative reports.
- `REMOVED`: Evidence omitted or absent in subsequent reports.
- `MODIFIED`: Evidence present across reports but assigned differing non-conflicting reviewer dispositions.
- `CONFLICTING`: Opposing reviewer dispositions (e.g. `SUPPORTING` vs. `CONFLICTING`) on the same evidence ID.
- `UNAVAILABLE`: Evidence explicitly marked `MISSING` or unavailable in one or more reports.

### B. Observation Diff (Preserving Epistemic Types)
Observations are tracked preserving their original ontological category:
- `FACT`, `INTERPRETATION`, `UNCERTAINTY`, `LIMITATION`, `QUESTION`.
- **Classification Changes:** Reclassifying a `FACT` as an `INTERPRETATION` is explicitly detected as `ObservationDiffType.CLASSIFICATION_CHANGE`, never treated as equivalent content.

### C. Data Quality Comparison
Evaluates:
- Quality status consistency (`all_valid`, `status_divergence`).
- Freshness drift (seconds elapsed between report baselines).
- Completeness ratio change with zero-baseline protected percentage delta (`safe_percentage_delta`).
- Anomaly counts and data corruption flags.

### D. Provenance Comparison
Evaluates:
- Identity of data sources, feeds, and providers.
- Source timestamps and lineage depth.
- Identifies differing sources or missing provenance records.

### E. Reproducibility Comparison
Evaluates:
- Reproducibility IDs and configuration fingerprints.
- Identifies shared vs differing source analysis run IDs.
- Reports `is_fully_reproducible` boolean status.

### F. Analytical Conclusions Comparison
Evaluates whether human reviewers reached agreeing or divergent conclusions (`conclusions_agree`), recording respective conclusion states and rationales without ranking or expressing trading preferences.

---

## 5. Review Consistency & Discrepancy Model

### InvestigationDiscrepancy
Every detected analytical discrepancy is modeled deterministically:
- `discrepancy_id`: Deterministic identifier.
- `category`: `EVIDENCE_DISPOSITION`, `FACT_INTERPRETATION_MISMATCH`, `UNCERTAINTY_UNRESOLVED`, `LIMITATION_UNACKNOWLEDGED`, `DATA_QUALITY_DEGRADATION`, `PROVENANCE_MISMATCH`, `CONCLUSION_DIVERGENCE`, `TIMEFRAME_MISMATCH`, `SYMBOL_MISMATCH`.
- `severity`: `INFO`, `WARNING`, `ERROR`, `CRITICAL`.
- `report_ids`: Tuple of reports involved in the discrepancy.
- `evidence_refs`: Specific evidence items in question.
- `description`: Objective explanation of the discrepancy.
- `resolution_state`: `UNRESOLVED`, `EXPLAINED`, `RESOLVED`, `NOT_APPLICABLE` (never auto-inferred).

### ConsistencySummary
Summarizes consistency metrics without generating scores or trade weights:
- `total_reports_compared`
- `comparable_reports`, `partially_comparable_reports`, `incompatible_reports`
- `shared_evidence_count`, `changed_evidence_count`, `conflicting_evidence_count`
- `unresolved_discrepancy_count`
- `new_uncertainty_count`, `resolved_uncertainty_count`, `common_limitation_count`

---

## 6. Deterministic Identity & Numerical Safety

### Deterministic Comparison ID
- Generated via SHA-256 fingerprinting of sorted canonical inputs:
  - Canonical report IDs (sorted alphabetically)
  - Report versions and report statuses
  - Canonical comparison scope and reproducibility IDs
- Re-running comparison on the same inputs produces the exact same `cmp_<hash[:16]>` ID.
- Idempotent and thread-safe.

### Numerical Integrity & Prohibited Field Rejection
- **Numerical Integrity:** Rejects `NaN`, `Infinity`, `-Infinity`. Strictly preserves `0` and `0.0`. Protects against zero-baseline division in percentage deltas.
- **Prohibited Fields:** Strongly rejects any field matching `action`, `side`, `order`, `entry`, `exit`, `quantity`, `position_size`, `leverage`, `stop_loss`, `take_profit`, `broker_order`, `execution_instruction`, `expected_profit`, `profit_probability`, `trade_probability`, `predicted_pnl`, `trade_signal`, `trade_ranking`, or `profitability_ranking`.

---

## 7. Handlers & MasterOrchestrator Integration

Five dedicated command handlers are registered with the `MasterOrchestrator`:
1. `compare_investigation_reports`: Executes cross-investigation comparison over a sequence of report IDs or reports.
2. `get_investigation_comparison`: Retrieves a comparison record by its deterministic comparison ID.
3. `list_investigation_comparisons`: Lists comparisons with bounded pagination and filtering.
4. `get_investigation_comparison_discrepancies`: Fetches structured discrepancies for a specific comparison ID.
5. `validate_investigation_comparison`: Performs deep structural validation, numerical integrity, and prohibited field checks.

All handlers are read-only with respect to source reports, enforce bounded inputs, and never invoke execution paths.

---

## 8. EventBus & StateStore Integration

### EventBus Events
Lifecycle events added to `EventType`:
- `INVESTIGATION_REPORTS_COMPARED`: Published upon successful comparison generation.
- `INVESTIGATION_COMPARISON_FAILED`: Published when validation or execution fails.
- `INVESTIGATION_DISCREPANCY_DETECTED`: Published when unresolved discrepancies are identified.

All event payloads are sanitized: no credentials, API keys, raw transcripts, or absolute file paths are published.

### StateStore Retention
Persisted with bounded retention:
- `investigation_comparison:latest`: Most recent comparison record.
- `investigation_comparison:<comparison_id>`: Specific comparison indexed by ID.
- `investigation_comparison:history`: Bounded ring-buffer of recent comparisons (max 100). Historical records are never overwritten.

---

## 9. Dashboard API Endpoints

Loopback-only (strictly `127.0.0.1` / `localhost`), read-only endpoints:
- `GET /api/investigation-comparisons`: List comparisons with pagination and filtering.
- `GET /api/investigation-comparisons/<comparison_id>`: Retrieve specific comparison record.
- `GET /api/investigation-comparisons/<comparison_id>/discrepancies`: Retrieve discrepancy records.
- `HEAD`: Supported on all endpoints (returning identical headers with empty body).
- `POST`, `PUT`, `DELETE`: Explicitly rejected with `405 Method Not Allowed`.
- Protection against directory traversal, null bytes, oversized paths, and malformed identifiers.

---

## 10. Voice Summary

Generates speech-ready, non-directional summaries for voice interaction:
- **Structure:** Deterministic template summarizing comparability state, reports compared, evidence consensus, and unresolved discrepancies.
- **Example:**
  > *"KAISER cross-investigation comparison complete. 2 investigation reports are COMPARABLE for BTC-USD. Found 2 shared evidence items, 1 conflicting evidence items, and 1 unresolved discrepancies. Analytical conclusions agree across reports."*
- **Safety Invariant:** Never utters execution terms (`BUY`, `SELL`, `HOLD`, `LONG`, `SHORT`, `ENTER`, `EXIT`, `EXECUTE`, `ORDER`) or mentions expected profit/probabilities.

---

## 11. Verification & Testing

### A. Focused Test Suite
`tests/test_phase_2_step_28_investigation_comparison.py` covers 41 comprehensive tests:
1. `test_valid_comparable_reports` — Verified `COMPARABLE` state.
2. `test_partially_comparable_reports` — Timeframe mismatch yielding `PARTIALLY_COMPARABLE`.
3. `test_non_comparable_reports` — Symbol mismatch yielding `NOT_COMPARABLE`.
4. `test_invalid_report_input` — Invalid report state detection.
5. `test_incomplete_evidence` — Incomplete report state detection.
6. `test_shared_evidence` — Unchanged evidence matching across reports.
7. `test_added_evidence` — Detection of `ADDED` evidence.
8. `test_removed_evidence` — Detection of `REMOVED` evidence.
9. `test_modified_evidence` — Detection of `MODIFIED` evidence.
10. `test_conflicting_evidence` — Opposing reviewer dispositions detected as `CONFLICTING`.
11. `test_observation_differences` — Fact and interpretation diffs.
12. `test_fact_interpretation_classification_change` — Epistemic classification change detection.
13. `test_uncertainty_changes` — New and varying uncertainties.
14. `test_limitation_changes` — Differing limitations detected.
15. `test_provenance_comparison` — Provenance alignment verification.
16. `test_data_quality_comparison` — Quality status and completeness checks.
17. `test_reproducibility_comparison` — Run IDs and fingerprint matching.
18. `test_conclusion_comparison` — Analytical conclusion alignment.
19. `test_review_consistency` — Summary statistics computation.
20. `test_discrepancy_generation` — Multi-category discrepancy extraction.
21. `test_discrepancy_resolution_state` — Unresolved discrepancy states.
22. `test_deterministic_comparison_id` — Deterministic SHA-256 ID calculation.
23. `test_repeated_identical_comparison` — Idempotence check.
24. `test_stable_report_ordering` — Order independence of inputs.
25. `test_nan_rejection` — Strict rejection of `NaN`.
26. `test_infinity_rejection` — Strict rejection of `Infinity`.
27. `test_zero_preservation` — Genuine `0.0` preserved.
28. `test_numeric_zero_baseline_safety` — Division-by-zero protection.
29. `test_eventbus_events` — Publishing of `INVESTIGATION_REPORTS_COMPARED` and discrepancy events.
30. `test_statestore_persistence` — State store latest and history keys.
31. `test_handler_registration` — MasterOrchestrator handler resolution.
32. `test_concurrency` — Safe concurrent execution across 5 threads.
33. `test_dashboard_get` — Loopback HTTP GET endpoints.
34. `test_dashboard_head` — HTTP HEAD method support.
35. `test_dashboard_405` — Rejection of mutating requests.
36. `test_dashboard_traversal_protection` — Path traversal security.
37. `test_prohibited_field_rejection` — Strict validation rejection of trade fields.
38. `test_execution_lock_invariant` — `Settings.execution_locked` invariant.
39. `test_no_broker_initialization` — Broker provider absence check.
40. `test_no_autonomous_execution` — Invariant check against autonomous execution.
41. `test_juju_isolation` — Strict isolation verifying JUJU was not modified.

**Result: 41 passed in 2.26s.**

### B. Full Regression Suite
Ran `python -m pytest -q` across the entire project:
**Result: 1181 passed in 83.82s (0:01:23).**

### C. System Startup & Dashboard Verification
- `python run.py`: Clean startup, registered all comparison handlers, reported execution engine locked, exited with code 0.
- `Settings.execution_locked = True`: Confirmed invariant.
- Trading engine: `not initialized`.
- JUJU: Completely untouched.

---

## 12. Deliverable Files

| File | Status | Description |
|---|---|---|
| [`app/analysis/opportunities/comparison/__init__.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/comparison/__init__.py) | Created | Package export for comparison models, engine, and handlers |
| [`app/analysis/opportunities/comparison/models.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/comparison/models.py) | Created | Immutable domain models, enums, deterministic comparison ID computation, numerical validators |
| [`app/analysis/opportunities/comparison/engine.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/comparison/engine.py) | Created | Deterministic cross-investigation comparison engine, diffing, discrepancy generation, StateStore, EventBus |
| [`app/analysis/opportunities/comparison/handlers.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/comparison/handlers.py) | Created | MasterOrchestrator command handlers for comparison operations |
| [`app/core/events/event.py`](file:///c:/Users/user/Desktop/KAISER/app/core/events/event.py) | Modified | Added `INVESTIGATION_REPORTS_COMPARED`, `INVESTIGATION_COMPARISON_FAILED`, `INVESTIGATION_DISCREPANCY_DETECTED` to `EventType` |
| [`app/portfolio/dashboard/server.py`](file:///c:/Users/user/Desktop/KAISER/app/portfolio/dashboard/server.py) | Modified | Added endpoint 21 for investigation comparisons, attached engine to server, 405 on mutations |
| [`app/bootstrap.py`](file:///c:/Users/user/Desktop/KAISER/app/bootstrap.py) | Modified | Initialized `InvestigationComparisonEngine`, registered handlers, linked to dashboard and SystemStatus |
| [`app/analysis/opportunities/__init__.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/opportunities/__init__.py) | Modified | Exported comparison components |
| [`app/analysis/__init__.py`](file:///c:/Users/user/Desktop/KAISER/app/analysis/__init__.py) | Modified | Exported comparison components |
| [`tests/test_phase_2_step_28_investigation_comparison.py`](file:///c:/Users/user/Desktop/KAISER/tests/test_phase_2_step_28_investigation_comparison.py) | Created | 41 unit, integration, property, security, concurrency, and invariant tests |
| [`KAISER_PHASE_2_STEP_28_IMPLEMENTATION_REPORT.md`](file:///c:/Users/user/Desktop/KAISER/KAISER_PHASE_2_STEP_28_IMPLEMENTATION_REPORT.md) | Created | Comprehensive implementation report |

---

## 13. Exact Git Commit Hash

- **Git Commit Hash:** `785faf98ad9610b61175be69ee444b731ba7bf2c`

