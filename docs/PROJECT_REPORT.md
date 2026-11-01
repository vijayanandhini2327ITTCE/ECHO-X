# ECHO-X
## Temporal Intelligence & Forgotten History Reconstruction Engine

> *Connecting fragmented memories to reconstruct historical truth.*

**Live application:** https://echo-x-7474645617938118.aws.databricksapps.com
**Source code:** notebook `ECHO -X` (pipeline) · `apps/echo-x` (application) · this documentation set

---

## 1. Abstract

ECHO-X is an end-to-end data engineering and AI system built on Databricks that reconstructs
**forgotten software project history** from fragmented GitHub records — issues, pull requests,
and releases — using a Medallion Architecture (Bronze → Silver → Gold), PySpark, Delta Lake, and
Unity Catalog. It extracts structured historical events, entities, timelines, a knowledge graph,
reconstructed engineering decisions, and contradictions, then answers natural-language questions
with **evidence-grounded** answers via a Genie agent and a deployed Streamlit application.

The system's defining discipline is **ELTR (Evidence-Linked Temporal Reconstruction)**: every
fact is either *documented* (directly traceable to a source URL) or *inferred* (pattern-derived,
confidence-scored, and clearly labeled), contradictions preserve **both sides** instead of
silently resolving them, and what cannot be reconstructed is declared as an explicit **gap**.
On the current dataset and evaluation run, all 12 advanced benchmark questions scored 100% across
six dimensions with 0% unsupported (hallucinated) claims.

## 2. Problem Statement

Software projects accumulate history across issues, PRs, and releases, but that history is
fragmented: a decision's context is scattered across dozens of records, temporal evidence is
incomplete, and naive retrieval (e.g., plain RAG over documents) mixes documented facts with
guesses. Questions like *“Why was Click vendored into Typer?”* or *“What happened before release
0.26.0?”* require **temporal reasoning, entity resolution, and evidence traceability** — not just
semantic search. Existing chat-over-your-data systems frequently hallucinate dates, attribute
decisions to the wrong release, or silently drop contradictions. There is a need for a system
that reconstructs history while **provably never fabricating it**.

## 3. Project Objectives

1. Ingest GitHub REST API data (issues, PRs, releases) into a governed Lakehouse.
2. Clean and normalize records into a single typed Silver layer.
3. Extract structured events and entities (10 event types, 598 → 232 events + 726 entities).
4. Build temporal intelligence: timelines, incident-resolution chains, technology evolution.
5. Construct a knowledge graph (958 nodes / 1,618 edges) with documented vs inferred edges.
6. Reconstruct engineering decisions (19) with triggers, implementations, and outcomes.
7. Detect contradictions (2) and preserve both sides with evidence.
8. Assemble evidence-linked investigations (19) with confidence scores and declared gaps.
9. Serve answers through a Genie agent and an interactive application — always evidence-grounded.
10. Evaluate rigorously: 11 data-quality gates + 12-question automated benchmark suite.

## 4. Existing System Limitations

| Limitation of ordinary approaches | ECHO-X's answer |
| --- | --- |
| Plain RAG retrieves documents but cannot reason about time | Explicit event model with real timestamps only |
| LLMs fabricate dates/releases | Every event_time must equal a source-record timestamp (verified 100%) |
| Contradictions get silently resolved to one answer | Contradiction table preserves both sides + evidence |
| No separation of fact vs inference | documented/inferred labeling on every graph edge and fact |
| Answers lack provenance | Every answer carries evidence URLs traceable to GitHub |
| Unknown coverage is hidden | Declared gaps: “what could NOT be reconstructed” is part of the output |

## 5. Proposed System

ECHO-X proposes a **reconstruction pipeline** rather than a retrieval pipeline: raw records are
progressively transformed into an event/entity temporal knowledge graph, and answers are assembled
from that graph under the ELTR methodology. Answering is a two-tier design: a Databricks Genie
agent trained on the Gold tables provides narrative answers, and a deterministic ELTR engine
answers from the same tables without any generative model — guaranteeing the same evidence
guarantees even when Genie is unavailable.

## 6. System Architecture

```text
GitHub REST API
   │  (issues · pull requests · releases; pagination + rate-limit handling)
   ▼
BRONZE  3 raw tables (MERGE-idempotent on source_id)
   ▼
SILVER  silver_github_records — 598 typed, deduplicated, normalized records
   ▼
GOLD    events → entities → timeline → knowledge graph → decisions →
        contradictions → investigations → evaluation → benchmarks
   │
   ├── Genie Agent  “ECHO-X Temporal Intelligence Historical Reconstruction”
   └── Databricks App “echo-x” (Streamlit, 4 modules, read-only)
```

All data lives in Unity Catalog (`learn_get_started_de_vijayanandhini.echo_x`) with 18 tables.
See `ARCHITECTURE_DIAGRAMS.md` for full publication-quality diagrams.

## 7. Technology Stack

- **Platform:** Databricks (Free Edition) — serverless compute, Unity Catalog governance
- **Processing:** PySpark (Spark SQL, DataFrame API, window functions)
- **Storage:** Delta Lake tables in Unity Catalog
- **Ingestion:** GitHub REST API v3 (Python `requests`, pagination, anonymous rate limits)
- **AI:** Databricks Genie (Genie Agent with attached tables + evidence-grounding instructions)
- **App:** Streamlit on Databricks Apps; Plotly, streamlit-agraph; Databricks SDK
  (statement execution + Genie API, app service-principal OAuth)
- **Language:** Python 3.12, SQL

## 8. Dataset Description

Source: **`tiangolo/typer`** (now `fastapi/typer`) — the Typer CLI library; a real, evolving
open-source project with vendoring decisions, breaking releases, and bug history.

| Table | Rows | Content |
| --- | --- | --- |
| bronze_github_issues | 12 | Raw issues (recent-history cap: 5 pages) |
| bronze_github_pull_requests | 500 | Raw PRs (recent-history cap: 5 pages) |
| bronze_github_releases | 86 | Full release history 2019 → 2026 |
| silver_github_records | 598 | Unified typed records (unique `source_id`) |

Entity resolution case embedded in the data: `source_ids` say `tiangolo/typer` while all 598
`source_urls` point to `github.com/fastapi/typer` — the repository moved organizations. ECHO-X
detects and documents this as a genuine identity contradiction rather than hiding it.

## 9. Data Engineering Workflow

1. **Bronze (idempotent ingestion):** REST pagination with rate-limit handling; `MERGE` on
   `source_id` (convention: `gh:{owner}/{repo}:{type}:{number}`) so re-runs never duplicate.
2. **Silver (quality gate):** union + dedup, typed timestamps (`created_ts/updated_ts/closed_ts`),
   normalized state/author/labels, body-length features, payload JSON retained for reprocessing.
   Recomputed with overwrite **by design** — Silver is fully derived from Bronze.
3. **Gold (enrichment layers, append-safe):**
   - `gold_events` (232) — 10 rule-based event types over titles + labels; event_time is always a
     real source timestamp (created_ts or closed_ts — never interpolated).
   - `gold_entities` (726) — records + 117 developers + 11 technologies with mention counts.
   - `gold_timeline` (232) — global chronological sequence, 0 chronology violations.
   - `gold_incident_resolutions` (35) — bug → fix → release chains (25 resolved, avg 22.8 days).
   - `gold_technology_evolution` (43) — technology × year mention curves (e.g., Click 2019:1 →
     2026:58; Pydantic first seen 2026).
   - `gold_nodes` (958) / `gold_edges` (1,618) — knowledge graph (see §11).
   - `gold_decisions` (19) — decision reconstruction (see §13).
   - `contradiction_records` (2) — contradiction detection (see §14).
   - `gold_investigations` (19) — ELTR assembly (see §10).
   - `gold_evaluation_metrics` (11) — automated data-quality gates.
   - `gold_benchmark_questions/results/summary` (12/12/1) — benchmark suite (see §15).

## 10. ELTR Methodology — Evidence-Linked Temporal Reconstruction

ELTR is the system's core discipline. For each reconstructed decision:

1. **Timeline slicing** — gather events ±60 days around the decision.
2. **Verified facts** — only facts backed by documented edges (each carries an evidence URL).
3. **Inferred facts** — pattern-derived facts, each labeled with a confidence score.
4. **Contradiction scan** — overlapping contradictions are attached, both sides preserved.
5. **Confidence scoring** — `overall_confidence = 0.5·decision + 0.3·event-alignment +
   0.2·verified-facts bonus` → a reconstruction class (high / medium).
6. **Declared gaps** — `missing_information` explicitly lists what could **not** be reconstructed.

Result on the current dataset: 19 investigations (18 high-confidence/implemented, avg 0.80;
1 medium/unresolved at 0.68), 17 of 19 with honestly declared gaps — e.g., the flagship Click
vendoring investigation (PR #1774, confidence 0.81, 60 aligned events) declares
*“no alternatives discussion found”*.

## 11. Knowledge Graph Implementation

`gold_nodes` (958 = 232 event nodes + 726 entity nodes) and `gold_edges` (1,618, all unique,
`edge_id = md5(source|target|type)`; referential integrity: 0 orphan endpoints).

| Edge type | Count | Basis |
| --- | --- | --- |
| RESOLVED_BY (bug → fix) | 25 | documented |
| PRECEDED_BY (timeline adjacency) | 231 | inferred from sequence |
| SUPERSEDED_BY (consecutive releases) | 85 | inferred from release order |
| IMPLEMENTED_IN (fix → first later release) | 25 | inferred from timestamps |
| RELATED_TO (developer → record authorship) | 598 | documented |
| RELATED_TO (technology → mentioning record) | 654 | inferred from text mentions |

Every edge carries `relationship_basis ∈ {documented, inferred}`, a `confidence_score`, and
`evidence_source_id/url`. Hub nodes in the current data: dependabot[bot] (190), python (178),
tiangolo (161). Example chain: bug-discovery PR → fixed 21 minutes later → released 6 minutes
after that (release 0.27.3 window) — all timestamp-verified.

## 12. Temporal Intelligence

- **Timeline:** global event sequence with contiguous 1..232 ordering; 0 chronology violations.
- **Event times:** every `event_time` equals the `created_ts` or `closed_ts` of its source record
  (verified: `event_time_source_match_pct = 100%`) — the system never invents a date.
- **Incident chains:** 35 bug→fix→release chains (25 resolved, avg 22.8 days, max 220 days;
  unresolved chains are kept visible, not dropped).
- **Technology evolution:** 43 technology-year mentions; before/after analysis around any release
  (e.g., 6 bug discoveries + 8 bug resolutions within 14 days after release 0.26.0, including
  the envvar bug #1787 → traced to release 0.26.1).
- **Coverage honesty:** 7 data-coverage gaps > 90 days (largest 379 days) are reported as
  *coverage gaps*, never interpreted as “nothing happened”.

## 13. Decision Reconstruction

`gold_decisions` (19 decisions, `decision_id = 'decision:' + source_id`). For each decision the
system reconstructs: triggering problem (latest problem event in prior 30 days), related events
(±30 days), alternatives-mentioned flag, implementation release (first release ≤ 90 days after
closure), subsequent outcomes (+30 days), and evidence URL. Statuses are evidence-based:
18 implemented (confidence 0.75), 1 unresolved (open PR, confidence 0.5). *“Rejected” is not
derivable from the current Silver schema (merged/rejected distinction not extracted) — a
declared limitation, not a fabricated status.* Flagship: “Vendor Click and streamline Typer”
(PR #1774, 2026-05) → implemented in release 0.26.0 with 33 subsequent outcome events; related
aftermath: issue #1867 “public CLI error after vendoring Click” → resolved by 0.27.0.

## 14. Contradiction Detection

Four detection checks over Silver/Gold: **id-vs-URL identity**, **timestamp order**,
**same title different dates**, **same title different states**. Current findings (both unique,
both sides + evidence URLs preserved):

1. `identity_mismatch` — `tiangolo/typer` in source_ids vs `fastapi/typer` in all 598 URLs
   (confidence 0.9): the repository moved organizations.
2. `conflicting_event_dates` — “Drop support for Python 3.8” reported 2025-12-24 17:22 vs
   2025-12-25 09:27 (confidence 0.7).

Honest zeros: `date_order_inconsistency` 0, `conflicting_status` 0 — reported as *none found*,
not fabricated.

## 15. Evaluation Methodology

Two independent layers, both automated and reproducible:

1. **Data-quality gates (`gold_evaluation_metrics`, 11 metrics):** event/entity/decision/
   investigation/contradiction uniqueness 100%; evidence URL coverage 100%; event-time
   coverage 100%; event-time-source match 100%; chronology violations 0; traceability 100%;
   both-sides preservation 100%.
2. **Answer benchmarks (12 questions across 5 categories — knowledge graph, temporal, decision,
   contradiction, evidence synthesis):** each question scored per-dimension (correctness,
   evidence coverage, traceability, temporal accuracy, relationship accuracy, contradiction
   handling, unsupported-claim rate) with deterministic SQL checks against Gold. Results are
   append-only by `run_id`, so history is preserved.

## 16. Results and Performance

> **Scope note:** these are results from the **current dataset (typer, 598 Silver records) and
> the reported evaluation run (`run_20261101_060656`, 2026-11-01)** — not universal performance
> guarantees. Full details: `RESULTS_AND_EVALUATION.md`.

| Metric | Result (current dataset / run) |
| --- | --- |
| Silver records | 598 |
| Extracted events | 232 (10 types) |
| Entities | 726 (598 records + 117 developers + 11 technologies) |
| Knowledge-graph nodes / edges | 958 / 1,618 |
| Reconstructed decisions | 19 (18 implemented, 1 unresolved) |
| Contradictions detected | 2 (both sides preserved) |
| ELTR investigations | 19 (avg confidence 0.80) |
| Data-quality gates | 11 / 11 at target |
| Advanced benchmarks | **12 / 12 passed — 100%** on all six dimensions |
| Unsupported (hallucinated) claims | **0.0%** |

## 17. Application Modules

Deployed Streamlit app **echo-x** (read-only, live-metric, SSO-protected):

1. **🛰 Intelligence Dashboard** — live KPIs, events by type, technology evolution, bug→fix→
   release statistics, developer contributions, decision status, contradictions, benchmark bars.
2. **📜 Timeline Explorer** — chronological events with date/type/technology/developer/search
   filters; each event expands to its GitHub source URL and underlying Silver record.
3. **🕸 Knowledge Graph Explorer** — interactive graph from gold_nodes/gold_edges; documented
   (green) vs inferred (amber) edges; bug→fix→release chain browser.
4. **🔎 Investigation Assistant** — Genie narrative answers (with the evidence SQL Genie used) or
   the deterministic ELTR engine: verified facts, inferred facts, evidence URLs, confidence,
   declared gaps. Insufficient evidence produces an explicit statement — never a guess.

## 18. Installation and Execution

See `EXECUTION_GUIDE.md` for the full step-by-step reproduction (ingestion → Gold → Genie →
benchmarks → app deploy). Quick reference:

```bash
# app lifecycle (workspace source)
databricks apps get echo-x
databricks apps deploy echo-x --source-code-path /Workspace/Users/<user>/echo-x-app
# git-sourced deploy (current setup: vijayanandhini2327ITTCE/ECHO-X, apps/echo-x)
```

The app authenticates as its own Databricks service principal (no credentials in code) with
`GRANT SELECT ON SCHEMA echo_x` — read-only by construction.

## 19. Limitations

Full list in `LIMITATIONS_AND_ROADMAP.md`. Key items:

- Bronze ingestion caps issues/PRs at 5 API pages (recent history only; releases are complete).
- “Rejected” decision status not derivable from current Silver (merged/rejected not extracted).
- Genie answers depend on the app service principal having Genie space access; the app falls
  back to the deterministic ELTR engine otherwise (fallback verified).
- All 100% benchmark figures are specific to the current dataset and run — not generalization
  claims.

## 20. Future Enhancements

1. Full-history ingestion with a authenticated token + incremental Auto Loader ingestion.
2. Richer PR metadata (merged/rejected/review timestamps) to complete decision statuses.
3. Vector/semantic search over records as an additional Genie signal.
4. Multi-repo reconstructions and cross-project entity resolution.
5. Streaming updates (Auto Loader + Structured Streaming) for live history.
6. Evaluation regression runs scheduled as a Lakeflow Job.

---

*Documentation set: `README.md` · `TECHNICAL_AUDIT.md` · `ARCHITECTURE_DIAGRAMS.md` ·
`EXECUTION_GUIDE.md` · `RESULTS_AND_EVALUATION.md` · `LIMITATIONS_AND_ROADMAP.md` ·
`INTERVIEW_PREPARATION.md`*