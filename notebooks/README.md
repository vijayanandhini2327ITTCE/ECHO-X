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

## Getting the notebook into this folder

The notebook is exported from the Databricks workspace and committed here:

1. Open the notebook in the Databricks workspace.
2. **File → Export → Source file (.py)** (or `.ipynb` for Jupyter viewing).
3. Save it as `notebooks/ECHO-X-pipeline.py` and commit.

*(The notebook contains **no credentials** — GitHub ingestion is anonymous and all platform
auth is handled by the workspace, so it is safe to publish.)*

## Execution order & idempotency

- Run cells top-to-bottom; each phase validates its outputs before the next starts.
- Bronze re-runs are safe (MERGE on `source_id`), Silver is rebuilt by design (derived layer),
  Gold phases are deterministic, and benchmark results **append-only by `run_id`** — evaluation
  history is never overwritten.
