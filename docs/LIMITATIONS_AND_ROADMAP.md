# ECHO-X — Known Limitations and Future Roadmap

Honest limitations are a **feature** of ECHO-X: the same discipline applied to the data
(declared gaps, both-sides contradictions) is applied to the system itself.

## 1. Known limitations

### Data coverage
- **Ingestion cap:** anonymous GitHub API + `MAX_PAGES=5` → issues (12) and PRs (500) cover
  recent history only; releases (86) are complete 2019→2026. History before the cap window is
  absent, not “confirmed to be empty”.
- **Coverage gaps in time:** 7 gaps > 90 days without events (largest 379 days) — reported as
  coverage gaps, never as proof of absence.

### Extraction expressiveness
- **Rule-based extraction:** 10 event types via title regex + labels; subtle discussions that
  don't use the known keywords are not extracted (e.g., design_discussion currently 0 — no
  matching records in this dataset).
- **Decision statuses:** “rejected” is not derivable because Silver doesn't extract the
  merged/rejected distinction — the system reports implemented/unresolved only, and says so.
- **Inference limits:** inferred edges (PRECEDED_BY, SUPERSEDED_BY, IMPLEMENTED_IN,
  technology RELATED_TO) are pattern-based, always labeled + confidence-scored, but are not
  evidence-linked by definition.
- **Contradiction detection** covers 4 check families; contradictory narratives in prose are
  not semantically compared.

### AI and application
- **Genie dependency:** narrative answers depend on Genie availability and on the app service
  principal having Genie-space access (one-time UI share); without it the app automatically
  falls back to the deterministic ELTR engine (verified working — same evidence guarantees,
  less narrative fluency).
- **Genie answer latency:** typically ~30–60 s per question; the app polls with a timeout and
  falls back on expiry.
- **Benchmarks are dataset-scoped:** the 12/12 @ 100% run applies to this dataset and run; it is
  a methodology validation, not a generalization claim.
- **Anonymous auth:** no PAT secret is used; token-based ingestion would lift rate limits and
  the page cap.

### Platform
- Free Edition workspace: compute sizes, app compute, and warehouse options are constrained;
  designed to be small-data friendly (598 records). Scaling to millions of records would need
  partitioning/tuning review (e.g., the single-partition window warning in the timeline build).

## 2. Future enhancement roadmap

| Priority | Enhancement | Impact |
| --- | --- | --- |
| High | PAT-authenticated, full-history ingestion (remove 5-page cap) | Complete historical coverage |
| High | Incremental Auto Loader ingestion + Structured Streaming | Live/continuous history instead of batch |
| High | Extract PR merged/rejected + review timestamps into Silver | Complete decision statuses; better incident chains |
| Medium | Semantic contradiction detection (embedding comparison of prose claims) | Catches narrative contradictions, not just structural ones |
| Medium | Vector search index over records as an additional Genie retrieval signal | Richer narrative answers |
| Medium | Multi-repo ingestion + cross-project entity resolution | Compare ecosystem histories (e.g., fastapi + typer + click) |
| Medium | Scheduled benchmark regression runs (Lakeflow Job, weekly) | Continuous quality monitoring with run history |
| Low | LLM-assisted extraction (with human-verified rules) instead of regex-only | Subtler events; must preserve ELTR labeling guarantees |
| Low | App: saved investigations, exportable reports (PDF), multi-user annotations | Research-team usability |

## 3. What will NOT change (design invariants)

1. **No fabricated timestamps** — event times always equal real source-record timestamps.
2. **Documented vs inferred is always labeled** — never silently mixed.
3. **Contradictions always keep both sides** — never auto-resolved.
4. **Declared gaps are part of the output** — the system reports what it could not reconstruct.
5. **The application is read-only over Gold** — no write path exists.
6. **Evaluation is deterministic and append-only** — no subjective grading, no history loss.