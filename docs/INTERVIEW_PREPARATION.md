# ECHO-X — Technical Interview Preparation

Prepared for a professional project presentation and TCS Digital interview. Every topic is
explained at three levels: **🌱 Beginner** (simple words), **🔧 Intermediate** (technical
accuracy), **🎯 Interview-ready** (the answer you actually give, with ECHO-X-specific evidence).

Numbers cited are from the current dataset/evaluation run (see `RESULTS_AND_EVALUATION.md`).

---

## 1. Why Databricks?

**🌱 Beginner.** Databricks is one platform where I can store data, process it with Spark, keep
it governed, and even host my app — everything I needed for this project in one place.

**🔧 Intermediate.** Databricks provides a Lakehouse: Unity Catalog for governance, serverless
compute for PySpark, Delta Lake for ACID storage, Genie for AI-over-data, and Apps for
deployment — all sharing one security model. Without it I would have to glue together a data
lake, a Spark cluster manager, a catalog, an LLM gateway, and a web host.

**🎯 Interview-ready.** “ECHO-X needed four capabilities: governed batch processing, an AI layer
that answers over data with citations, evaluation infrastructure, and a secure way to serve a UI.
Databricks gave me all four under one governance model — Unity Catalog. My app's service
principal carries only `SELECT` grants, so the read-only guarantee is enforced by the platform,
not by my code. That's why I chose it over stitching together S3 + EMR + a vector DB + a web
service.”

## 2. Why PySpark?

**🌱 Beginner.** PySpark lets me process large datasets using Python by distributing work across
many machines, and it connects directly to Delta tables.

**🔧 Intermediate.** The workload is set-based transformation + window analytics (timelines,
chains, entity resolution) — exactly what Spark's DataFrame API and Spark SQL are built for.
PySpark gives that with Python ergonomics. Even at 598 records, the same code scales to
millions without redesign.

**🎯 Interview-ready.** “My temporal layer is built on window functions — global event ordering,
incident-resolution chains, release supersession. Those are relational operations, not ML or
graph algorithms, so PySpark is the natural fit. And because it targets Delta tables through
Unity Catalog, the pipeline is production-shaped from day one: the same notebook scales from my
598-record dataset to enterprise volume by changing nothing.”

## 3. Why Delta Lake?

**🌱 Beginner.** Delta Lake is a storage format that makes data tables reliable — safe updates,
versioning, and schema control.

**🔧 Intermediate.** Delta gives ACID transactions so MERGE-based idempotent ingestion is safe,
 time travel for auditing what the data looked like before any run, and schema enforcement so
a bad batch can't silently corrupt Gold tables.

**🎯 Interview-ready.** “Idempotency was a hard requirement — re-running ingestion must never
duplicate history. Delta's MERGE on a natural key (`source_id`) made Bronze idempotent, and
its transactional guarantees mean a failed run leaves no partial state. Every one of my 18
tables is a Delta table in Unity Catalog, so lineage, grants, and audit all come for free.”

## 4. Why Medallion Architecture?

**🌱 Beginner.** It organizes data in three quality layers — raw (Bronze), cleaned (Silver),
enriched (Gold) — so problems are caught early and each layer has one clear job.

**🔧 Intermediate.** Separation of concerns: Bronze preserves raw API payloads (reprocessable
forever), Silver enforces typing/dedup once, and Gold layers each own one enrichment concern
(events, entities, graph, decisions). A bug in Gold never forces re-ingestion; a Silver change
replays from Bronze without hitting the API again.

**🎯 Interview-ready.** “In ECHO-X, Bronze is MERGE-idempotent raw ingestion, Silver is a single
598-row typed union rebuilt from Bronze on every run by design — it's always reproducible —
and Gold is 15 tables, each a deterministic enrichment with provenance back to `source_id`.
The payoff was concrete: when I found the repo's entity-resolution contradiction
(tiangolo vs fastapi org), I could trace it from a Gold finding through Silver to the exact raw
records, in one query.”

## 5. Why a knowledge graph instead of ordinary RAG alone?

**🌱 Beginner.** RAG finds documents similar to a question; a knowledge graph stores actual
relationships — who authored what, which bug was fixed by which release — so answers come from
structure, not just text similarity.

**🔧 Intermediate.** Questions like “what happened before release 0.26.0?” are multi-hop and
temporal. Chunk-similarity retrieval can't guarantee the right hops, can't reason about time,
and can't tell the model what it doesn't know. My graph stores 958 nodes/1,618 edges with typed
relations (RESOLVED_BY, IMPLEMENTED_IN, SUPERSEDED_BY, RELATED_TO) plus a documented/inferred
basis and confidence on every edge — retrieval becomes traversal.

**🎯 Interview-ready.** “RAG retrieves text; ECHO-X retrieves relationships. My answer path is:
Genie queries Gold, where a bug is linked to its fix by a RESOLVED_BY edge and to the first
later release by IMPLEMENTED_IN — each edge labeled documented or inferred with confidence.
That's why temporal and multi-hop questions score 100% traceability in my benchmark run,
while a pure-RAG baseline would have to hope the right chunks were retrieved. The graph is
also what makes 'I don't know' possible: if no edge exists, the answer says so.”

## 6. How does temporal reasoning work?

**🌱 Beginner.** Every event gets a real date from its source record, events are sorted into a
timeline, and questions about 'before/after X' become simple date arithmetic.

**🔧 Intermediate.** Event times come only from real record timestamps (`created_ts`/
`closed_ts`) — never interpolated. A window function produces the global timeline (sequence
1..232, 0 chronology violations). Incident chains link bug→fix→release by timestamp ordering;
release-relative windows (±30/±60 days) answer 'what happened around release X'. Coverage gaps
(7 gaps > 90 days) are explicitly reported rather than treated as 'nothing happened'.

**🎯 Interview-ready.** “The core rule: no fabricated timestamps. Every event_time equals a
source-record timestamp — I verify this with an automated gate at 100%. Temporal questions
then reduce to timestamp arithmetic over a verified timeline — e.g., 6 bug discoveries and 8
bug resolutions within 14 days after release 0.26.0, and the envvar bug #1787 tracing to
release 0.26.1. And when the data has gaps, I report them as gaps — 7 gaps over 90 days,
largest 379 days — because 'absence of evidence' must never become 'evidence of absence'.”

## 7. How does ELTR work?

**🌱 Beginner.** ELTR — Evidence-Linked Temporal Reconstruction — is my method for rebuilding
history: gather events around a decision, keep only facts backed by evidence, label everything
else as inference with a confidence score, and explicitly list what could not be recovered.

**🔧 Intermediate.** For each of 19 decisions: slice events ±60 days; split facts into verified
(documented edges, each with an evidence URL) vs inferred (pattern-based, confidence-labeled);
attach overlapping contradictions with both sides; compute overall_confidence =
0.5·decision + 0.3·event-alignment + 0.2·verified-bonus; emit a reconstruction class and a
`missing_information` gap list.

**🎯 Interview-ready.** “ELTR is the pipeline's honesty mechanism. A produced investigation has
five sections: verified facts with URLs, labeled inferences, contradiction links, a confidence
score with its formula, and declared gaps. Example: the Click vendoring decision (PR #1774)
comes out at 0.81 confidence with 60 aligned events — and an honest gap: 'no alternatives
discussion found'. 17 of my 19 investigations declare gaps. That's the differentiator — most
systems answer; ECHO-X also tells you how much of the answer it could actually reconstruct.”

## 8. How are contradictions detected?

**🌱 Beginner.** Four automated checks compare records with each other — e.g., “same title but
different dates”, or “the ID says one owner but the URL says another” — and every hit is kept
with **both** versions and links to prove both.

**🔧 Intermediate.** Check families: id-vs-URL identity, timestamp ordering, same-title-
different-dates, same-title-different-states. Findings get a category, both values, affected
records, confidence, and evidence URLs. The two real findings: the repository's org migration
(tiangolo/typer in source_ids vs fastapi/typer in all 598 URLs, 0.9) and a “Drop Python 3.8
support” date clash (2025-12-24 17:22 vs 2025-12-25 09:27, 0.7). Checks with no hits report
honest zeros — never 'none found' by omission.

**🎯 Interview-ready.** “Two examples I can demo. First: my own data contained a genuine entity-
resolution contradiction — the repo moved from tiangolo to the fastapi org, so IDs and URLs
disagree across all 598 records. A naive pipeline would silently normalize one side; ECHO-X
records the conflict with both sides and evidence links at 0.9 confidence. Second: one issue
reports 'Drop Python 3.8 support' with two timestamps 16 hours apart — both preserved. The point
is philosophical: in history reconstruction, a contradiction is a finding, not a bug to hide.”

## 9. How is evidence traceability maintained?

**🌱 Beginner.** Every fact carries a link back to the GitHub record it came from, so anyone can
verify any answer in one click.

**🔧 Intermediate.** The chain is: Gold row → `source_id` (convention `gh:owner/repo:type:n`) →
`source_url` → GitHub. Graph edges carry their own `evidence_source_id/url`; investigations
carry arrays of evidence URLs. Automated gates verify `evidence_url_coverage_pct = 100%` and
`traceability_pct = 100%`; the benchmark suite independently re-checks traceable URLs per
answer.

**🎯 Interview-ready.** “Traceability is structural, not cosmetic: the provenance columns are
mandatory at every layer, verified by automated gates, and re-verified per-answer by my 12-
question benchmark run at 100%. In the app, every timeline event expands to its GitHub source
and even the underlying Silver record with its state, author, and timestamps. If I can't show
you the URL, it doesn't count as a fact.”

## 10. How are hallucinations controlled?

**🌱 Beginner.** The system only states facts it can prove with evidence; anything else is
labeled as an inference with a confidence score, and missing knowledge is declared openly.

**🔧 Intermediate.** Layered controls: (1) no fabricated timestamps (verified source match 100%);
(2) documented vs inferred labeling on every edge/fact; (3) both-sides contradictions;
(4) declared gaps (`missing_information`); (5) Genie instructions enforce evidence-grounding
and refusal when evidence is insufficient; (6) the app's deterministic tier answers without any
generative model at all; (7) a dedicated benchmark dimension counts **unsupported claims** —
currently 0.0%.

**🎯 Interview-ready.** “I treat hallucination control as an architecture property, not a prompt
guard. Five structural controls: timestamps must match source records, facts are either
documented or labeled inferred, contradictions keep both sides, gaps are declared, and the
evaluation suite has a dedicated dimension that counts unsupported claims. In the reported run
that dimension is 0.0% across 12 questions. And when Genie isn't available, the app falls back
to a deterministic engine that cannot hallucinate by construction — it only renders rows that
exist in Gold.”

## 11. How is the system evaluated?

**🌱 Beginner.** Two layers of automated tests: data-quality checks on the tables, and a 12-
question exam scored against known-right answers.

**🔧 Intermediate.** Layer 1: 11 data-quality gates (uniqueness of events/entities/decisions/
investigations/contradictions, evidence URL coverage, event-time coverage and source-match,
chronology violations, traceability, both-sides preservation) — all at target. Layer 2: 12
benchmark questions across 5 categories (knowledge graph, temporal, decision, contradiction,
evidence synthesis) scored per-dimension (correctness, evidence coverage, traceability,
temporal accuracy, relationship accuracy, contradiction handling, unsupported-claim rate) with
deterministic SQL assertions — no LLM grading. Results append by `run_id`, preserving history.

**🎯 Interview-ready.** “Evaluation is deterministic and reproducible — no LLM grades itself.
Two independent layers: 11 table-level gates, then a 12-question suite scored by SQL assertions
against Gold, including a dedicated hallucination dimension. Run `run_20261101_060656`:
12/12 passed, 100% on all six scored dimensions, 0% unsupported claims. Because every run
appends with its own ID, the suite doubles as a regression harness for future pipeline
changes. And I scope the claim honestly: it's 100% on this dataset and run — a validation of
the methodology, not a universal guarantee.”

## 12. What are the limitations?

**🌱 Beginner.** Ingestion is capped by anonymous API limits, some decision information isn't
in the data, and Genie answers depend on permissions — all documented openly.

**🔧 Intermediate.** Coverage: issues/PRs capped at 5 API pages (recent history); 7 temporal
gaps > 90 days. Expressiveness: regex/label-based extraction misses unkeyworded discussions;
'rejected' decisions not derivable from Silver; contradiction detection is structural, not
semantic. AI: Genie needs a one-time space-permission share for the app SP; narrative answers
take ~30–60 s. All benchmark figures are dataset-scoped. (Full list: LIMITATIONS_AND_ROADMAP.md.)

**🎯 Interview-ready.** “I'll name three. First, coverage: anonymous API access caps issues and
PRs at five pages — so my reconstruction is of recent history; releases are complete. Second,
expressiveness: extraction is rule-based, so it can't catch discussions that never use the
expected keywords — I report the honest zero rather than pretend. Third, decision status:
'rejected' isn't derivable because GitHub PR merged/rejected distinction isn't in my Silver
schema — I declare it as a limitation instead of inventing the status. Each has a roadmap item:
authenticated ingestion, LLM-assisted extraction under the same ELTR labeling, and richer PR
metadata.”

---

## 13. The 60-second elevator pitch

“ECHO-X reconstructs forgotten software history from fragmented GitHub records. It's built on
Databricks with a Medallion Architecture — Bronze, Silver, and 15 Gold tables in Unity Catalog.
From 598 records it extracts 232 timestamped events, 726 entities, and a knowledge graph with
958 nodes and 1,618 edges, then reconstructs 19 engineering decisions, detects 2 contradictions
— preserving both sides — and assembles 19 evidence-linked investigations under my ELTR
methodology. The key discipline: every fact is either documented, with a GitHub URL, or labeled
inference with a confidence score, and every investigation declares what it could NOT recover.
Answers are served by a Genie agent and a deployed Streamlit app — both strictly read-only.
On the reported 12-question benchmark run, it scored 100% across all six dimensions with 0%
unsupported claims — verified by deterministic SQL checks, not by an LLM grading itself.”

## 14. Quickfire Q&A (likely follow-ups)

| Question | Key point of your answer |
| --- | --- |
| “Where would this break at scale?” | Single-partition window for the timeline would need re-partitioning; otherwise Spark-native design scales |
| “Why not fine-tune an LLM instead?” | Governance and verifiability: my answers must be auditable; a fine-tune can't guarantee traceability or 'I don't know' |
| “Is Genie doing the reasoning?” | No — the graph and ELTR tables do; Genie is the narrative + NLQ surface over governed tables |
| “What was the hardest bug?” | Schema inference on all-None release columns breaking createDataFrame → fixed with explicit all-string StructType |
| “Why MERGE instead of append?” | Idempotency: re-runs must not duplicate history; MERGE on natural key `source_id` guarantees it |
| “What would you do with 6 more weeks?” | Authenticated full-history ingestion, semantic contradiction detection, scheduled benchmark regressions |
| “How do I trust your 100%?” | Deterministic SQL scoring, append-only run history, and the scope note: current dataset + run, verifiable by re-running cells 28–32 |