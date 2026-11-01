# ECHO-X — Results and Evaluation Report

> **Scope:** All results below are from the **current dataset** (tiangolo/typer GitHub records,
> 598 Silver rows) and the **reported evaluation run** (`run_20261101_060656`, executed
> 2026-11-01 06:07 UTC). They are **not universal performance guarantees** — they demonstrate
> the methodology works end-to-end on this data and are re-verifiable by re-running the
> benchmark cells (results append by `run_id`, history is preserved).

## 1. Reconstructed knowledge (current dataset)

| Metric | Value | Source table |
| --- | --- | --- |
| Historical records ingested | 598 | silver_github_records |
| Extracted events | 232 | gold_events |
| Entities | 726 (598 records · 117 developers · 11 technologies) | gold_entities |
| Knowledge-graph nodes | 958 | gold_nodes |
| Knowledge-graph edges | 1,618 | gold_edges |
| Reconstructed decisions | 19 (18 implemented · 1 unresolved) | gold_decisions |
| Contradictions detected | 2 (both sides preserved) | contradiction_records |
| ELTR investigations | 19 (avg confidence 0.797) | gold_investigations |
| Incident chains | 35 (25 resolved · avg 22.8 days) | gold_incident_resolutions |
| Technology evolution records | 43 | gold_technology_evolution |

## 2. Data-quality gates — 11 / 11 at target

| Gate | Result |
| --- | --- |
| event_uniqueness_pct | 100.0 |
| entity_uniqueness_pct | 100.0 |
| decision_uniqueness_pct | 100.0 |
| investigation_uniqueness_pct | 100.0 |
| contradiction_uniqueness_pct | 100.0 |
| evidence_url_coverage_pct | 100.0 |
| event_time_coverage_pct | 100.0 |
| event_time_source_match_pct (0 fabricated dates) | 100.0 |
| chronology_violations | 0.0 |
| traceability_pct | 100.0 |
| contradictions_both_sides_pct | 100.0 |

## 3. Advanced benchmark run — 12 / 12 passed, 100% on all dimensions

Run `run_20261101_060656` · 12 challenging questions across **5 categories**: knowledge graph,
temporal, decision, contradiction, evidence synthesis. Each question scored per-dimension with
deterministic SQL checks against Gold (no LLM grading — fully reproducible).

| Dimension | Result |
| --- | --- |
| Correctness (answer_accuracy) | 100.0% |
| Evidence coverage | 100.0% |
| Source traceability | 100.0% |
| Temporal accuracy | 100.0% |
| Relationship accuracy | 100.0% |
| Contradiction handling | 100.0% |
| **Unsupported (hallucinated) claims found** | **0.0%** |

Spot-verified cases from the run:

- **Temporal trace:** envvar bug #1787 correctly traced to release 0.26.1.
- **Relationship quality:** documented vs inferred relationships distinguished correctly in
  knowledge-graph questions.
- **Contradiction handling:** both contradiction cases answered with both sides + evidence URLs.
- **Decision recall:** unresolved decisions (e.g., PR #1934 “Keep the sub-app's deprecated flag
  in add_typer()”) matched pipeline evidence with declared gaps.

## 4. Genie-space benchmarks (independent of the notebook suite)

3/3 Genie benchmark questions scored **100%** at space-configuration time (run from the
Genie space UI).

## 5. Flagship demonstration — Click vendoring (PR #1774)

Question: *“Why was Click vendored into Typer?”* Answer assembled by ELTR:

- **Verified facts:** the decision PR #1774 “Vendor Click and streamline Typer” (2026-05),
  implemented in release 0.26.0 (33 subsequent outcome events); evidence URLs for both.
- **Inferred facts:** labeled with confidence; follow-on issue #1867 “public CLI error after
  vendoring Click” resolved by 0.27.0.
- **Confidence:** 0.81 (high). 60 aligned events in the ±60-day slice.
- **Declared gap:** *“no alternatives discussion found”* — the system states what it could not
  recover instead of guessing.

## 6. Evaluation design rationale

- **Deterministic grading:** every check is a SQL assertion against Gold — no subjective or
  LLM-graded scoring.
- **Multi-dimensional:** correctness alone is insufficient; traceability, temporal accuracy,
  relationship accuracy, contradiction handling, and a dedicated hallucination dimension are
  reported separately.
- **Append-only history:** each run appends by `run_id`; improvements and regressions across
  runs are comparable.
- **Honest zeros:** dimensions where the dataset has no cases (e.g., design_discussion events)
  are reported as 0/none-found, never fabricated.

## 7. How to re-run the evaluation

1. Notebook cells 28–31 build the idempotent question bank and score a new run.
2. Cell 32 renders the full report (dimension breakdown + category rollup + chart).
3. Compare runs via `gold_benchmark_summary` (all runs retained).