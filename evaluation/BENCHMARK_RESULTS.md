# ECHO-X — Evaluation Artifacts

> **Scope:** results from the current dataset (tiangolo/typer GitHub records, 598 Silver rows)
> and the reported evaluation run `run_20261101_060656` (2026-11-01). These validate the
> methodology on this data — they are not universal performance guarantees. The suite is
> deterministic and append-only by `run_id`, so it doubles as a regression harness.

## Advanced benchmark run — 12 / 12 passed

12 questions across 5 categories (knowledge graph · temporal · decision · contradiction ·
evidence synthesis), each scored per-dimension with deterministic SQL assertions against the
Gold tables — no LLM grades itself.

| Dimension | Score |
| --- | --- |
| Answer correctness | 100% |
| Evidence coverage | 100% |
| Source traceability | 100% |
| Temporal accuracy | 100% |
| Relationship accuracy | 100% |
| Contradiction handling | 100% |
| **Unsupported (hallucinated) claims found** | **0.0%** |

Spot-verified cases from the run: the envvar bug #1787 correctly traced to release 0.26.1;
documented vs inferred relationships distinguished correctly; both contradiction cases answered
with both sides + evidence URLs; unresolved decisions matched pipeline evidence with declared gaps.

## Data-quality gates — 11 / 11 at target

| Gate | Result |
| --- | --- |
| event / entity / decision / investigation / contradiction uniqueness | 100% each |
| evidence_url_coverage_pct | 100% |
| event_time_coverage_pct | 100% |
| event_time_source_match_pct (no fabricated dates) | 100% |
| chronology_violations | 0 |
| traceability_pct | 100% |
| contradictions_both_sides_pct | 100% |

## Reconstructed knowledge (current dataset)

598 Silver records · 232 events · 726 entities · 958 graph nodes · 1,618 edges ·
19 decisions · 2 contradictions · 19 ELTR investigations (avg confidence 0.80) ·
35 incident chains · 43 technology-evolution records.

## Genie-space benchmarks (independent)

3/3 Genie benchmark questions scored 100% at space-configuration time.

## Reproducing

Re-run notebook cells 28–32 (see `../docs/EXECUTION_GUIDE.md` §3): the question bank is
idempotent (MERGE), and each new run appends its results with a fresh `run_id` — history is
preserved and runs are comparable.
