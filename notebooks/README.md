# notebooks/ — The ECHO-X Pipeline

The full ECHO-X pipeline lives in a **Databricks notebook (`ECHO -X`, 33 cells)** that was built,
executed, and validated phase-by-phase. Every phase's outputs are Delta tables in Unity
Catalog (catalog `learn_get_started_de_vijayanandhini`, schema `echo_x`).

## Pipeline map (33 cells)

| Cells | Phase | Output tables |
| --- | --- | --- |
| 1–6 | Bronze ingestion (GitHub REST, MERGE-idempotent) | bronze_github_issues / pull_requests / releases |
| 7–8 | Silver cleaning | silver_github_records (598) |
| 9–12 | Event + entity extraction | gold_events (232) · gold_entities (726) |
| 13–15 | Temporal intelligence | gold_timeline · gold_incident_resolutions · gold_technology_evolution |
| 16–18 | Knowledge graph | gold_nodes (958) · gold_edges (1,618) |
| 19–20 | Decision reconstruction | gold_decisions (19) |
| 21–22 | Contradiction detection | contradiction_records (2) |
| 23–24 | ELTR investigations | gold_investigations (19) |
| 25–26 | Evaluation + NLQ dry-run | gold_evaluation_metrics (11 gates) |
| 27 | Genie agent setup instructions | (Genie space) |
| 28–32 | Advanced benchmarking | gold_benchmark_questions / results / summary |
| 33 | Evaluation report + charts | — |

The cell-by-cell description with key outputs is in
[`../docs/EXECUTION_GUIDE.md`](../docs/EXECUTION_GUIDE.md) §1.

## The export — `ECHO-X-pipeline.py` (present)

The real pipeline notebook is exported verbatim to [`ECHO-X-pipeline.py`](./ECHO-X-pipeline.py):

- **Source:** Databricks workspace notebook `ECHO -X`
  (`/Users/vijayanandhini@student.tce.edu/ECHO -X`), exported via the workspace export API in
  SOURCE format — the implementation, cell boundaries (`# COMMAND ----------`), comments and
  execution order are preserved exactly as built and executed.
- **Size:** 2,204 lines · 33 cells (32 `# COMMAND` separators) · ~114 KB.
- **Re-importable:** the first line is `# Databricks notebook source`, so the file can be
  imported back into a Databricks workspace as a notebook; it is also valid standalone
  Python syntax (verified with `py_compile`).
- **Dependencies:** a Databricks Runtime with PySpark, Spark SQL and `dbutils` (any recent
  DBR), plus the `requests` library (preinstalled on DBR) for the GitHub REST ingestion
  cells. No other packages are required.
- **Credentials:** none. The optional GitHub PAT is read at runtime with
  `dbutils.secrets.get(scope="github", key="pat")` and falls back to anonymous access —
  no secret values exist in the file. Platform auth is handled by the workspace.
- **Execution:** run top-to-bottom (same order as the map above). Set `CATALOG` in cell 1 to
  a catalog you can `CREATE SCHEMA` in; all Bronze writes are MERGE-idempotent on `source_id`.
- **What it produces:** the full `echo_x` schema — 3 Bronze + 1 Silver + 14 Gold tables
  (benchmarks included) — 18 Delta tables in total.

The benchmark numbers printed by the final cells are from the recorded evaluation run of
this dataset (`run_20261101_060656`, 12/12 passed) — a methodology validation, not a
universal accuracy guarantee. No notebooks were left un-exported: this single notebook is
the entire pipeline.

## Execution order & idempotency

- Run cells top-to-bottom; each phase validates its outputs before the next starts.
- Bronze re-runs are safe (MERGE on `source_id`), Silver is rebuilt by design (derived layer),
  Gold phases are deterministic, and benchmark results **append-only by `run_id`** — evaluation
  history is never overwritten.
