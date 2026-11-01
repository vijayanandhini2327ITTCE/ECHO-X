# ECHO-X — Project Execution Guide

End-to-end reproduction guide, from an empty workspace to the deployed application.
Estimated effort: 2–3 working days for the pipeline (mostly execution time), plus app deploy.

## 0. Prerequisites

- Databricks workspace (Free Edition suffices) with Unity Catalog.
- A catalog + schema, e.g. `learn_get_started_de_vijayanandhini.echo_x` (set `CATALOG`/
  `SCHEMA` in notebook cell 1 — CATALOG/SCHEMA are redefined defensively in cell 28 for kernel
  restarts).
- A serverless SQL warehouse (any size; a 2X-Small starter warehouse works for this data volume).
- Internet access to `api.github.com` from compute (anonymous access; a PAT raises rate limits
  and removes the 5-page cap if you want full history).

## 1. Pipeline (notebook `ECHO -X`, 33 cells — run top-to-bottom)

| Cells | Phase | What it builds | Key outputs |
| --- | --- | --- | --- |
| 1–6 | Bronze ingestion | 3 raw tables (MERGE-idempotent) | 12 / 500 / 86 rows |
| 7–8 | Silver cleaning | `silver_github_records` | 598 typed records |
| 9–12 | Event + entity extraction | `gold_events`, `gold_entities` | 232 events · 726 entities |
| 13–15 | Temporal intelligence | timeline · incident chains · evolution | 232 · 35 · 43 rows |
| 16–18 | Knowledge graph | `gold_nodes`, `gold_edges` | 958 · 1618 |
| 19–20 | Decision reconstruction | `gold_decisions` | 19 decisions |
| 21–22 | Contradiction detection | `contradiction_records` | 2 findings |
| 23–24 | ELTR investigations | `gold_investigations` | 19 investigations |
| 25–26 | Evaluation + NLQ dry-run | `gold_evaluation_metrics` | 11/11 gates at target |
| 27 | Genie setup instructions | (paste into Genie UI) | — |
| 28–32 | Advanced benchmarking | questions · results · summary · report | 12/12 @ 100% |
| 33 | Evaluation report (charts) | matplotlib report | category rollup |

Notes:
- Re-runs are safe: Bronze MERGEs, Silver overwrites by design, Gold phases are deterministic,
  benchmark results append-only by `run_id`.
- Every phase was executed and validated; cell outputs are retained in the notebook.

## 2. Genie Agent (manual, ~15 minutes — instructions in notebook cell 27)

1. Create a Genie space “ECHO-X Temporal Intelligence Historical Reconstruction”.
2. Attach all 11 gold tables; set descriptions + value indexing.
3. Paste the instructions (role, table guide, 6 evidence rules, no-fabrication rules).
4. Add starter questions; run the 3 benchmark questions (expected: 3/3 @ 100%).

## 3. Advanced benchmarks (notebook cells 28–32)

Run cells 28–31 in order (bank MERGE → per-question scoring → summary). Results are appended
with a new `run_id`; cell 32 renders the report. Current run: `run_20261101_060656` — 12/12,
100% on all six dimensions, 0.0% unsupported claims.

## 4. Application deploy

Source: `apps/echo-x` in the GitHub repo (current production source), or the workspace copy
`/Workspace/Users/<user>/echo-x-app`.

```bash
# 1. Inspect state first (deploy only succeeds when the app is RUNNING)
databricks apps get echo-x --output JSON
# If STOPPED: databricks apps start echo-x --timeout 20m

# 2. Deploy (choose ONE source)
# from workspace:
databricks apps deploy echo-x \
  --source-code-path /Workspace/Users/<user>/echo-x-app --output JSON
# from Git (current setup — configured in the App UI or via the app's git source):
#   repository vijayanandhini2327ITTCE/ECHO-X, branch main, path apps/echo-x

# 3. Verify
databricks apps get echo-x --output JSON   # app_status.state == RUNNING
```

App URL: https://echo-x-7474645617938118.aws.databricksapps.com (workspace SSO required).

## 5. Required permission grants (one-time)

```sql
-- read-only access for the app's service principal (use its UUID, not display name)
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-uuid>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.echo_x TO `<app-sp-uuid>`;
GRANT SELECT ON SCHEMA <catalog>.echo_x TO `<app-sp-uuid>`;
```

For the Genie answer engine: Genie space → **Share** → add the app's service principal
(`app-85dos0 echo-x` in this workspace) with **Can run**. Until this is done, the app
automatically uses the deterministic ELTR engine (fallback verified working).

## 6. Verification checklist after deploy

- App loads and shows the sidebar (ECHO-X, 5 pages).
- Dashboard KPIs match Gold: 598 records · 232 events · 726 entities · 958 nodes · 1,618 edges ·
  19 decisions · 2 contradictions · 19 investigations · benchmark 100%.
- Timeline: default range returns events; filters narrow them; expander shows GitHub source URL
  + Silver record detail.
- Graph: hub nodes render; green (documented) vs amber (inferred) edges; bug→fix→release chain
  browser works.
- Assistant: “Why was Click vendoring introduced?” answers with evidence URLs, confidence 0.81,
  and a declared gap (either engine).
- Tables untouched: row counts identical before/after (the app has no write grants at all).

## 7. Troubleshooting

| Symptom | Fix |
| --- | --- |
| `deploy` fails “not in RUNNING state” | Run `apps get` first; if STOPPED → `apps start`, if STARTING → wait |
| App pages show permission errors | Apply the three GRANT statements (§5) |
| Genie tier falls back | Share the Genie space with the app SP (§5) |
| Queries slow on first load | Serverless warehouse cold start (~1 min); cached for 5 min after |
| Statement timeout | Wait-timeout is 50 s with polling; retries happen automatically |