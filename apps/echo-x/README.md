# ECHO-X — Temporal Intelligence & Forgotten History Reconstruction Engine

**Tagline:** *Connecting fragmented memories to reconstruct historical truth.*

A read-only Streamlit research-intelligence app over the ECHO-X Gold layer
(catalog `learn_get_started_de_vijayanandhini`, schema `echo_x`) built with the
**ELTR (Evidence-Linked Temporal Reconstruction)** methodology.

## Modules

| Module | What it shows |
| --- | --- |
| 🛰️ Intelligence Dashboard | Live KPIs, events by type, technology evolution, bug→fix→release stats, developer contributions, decision status, contradictions, benchmark results |
| 📜 Timeline Explorer | Chronological events with filters (date, type, technology, developer, search) and per-event GitHub provenance |
| 🕸️ Knowledge Graph Explorer | Interactive graph from `gold_nodes`/`gold_edges`; documented vs inferred edges color-coded; bug→fix→release chains |
| 🔎 Investigation Assistant | Genie narrative answers with evidence SQL, plus a deterministic ELTR fallback (verified facts, inferred facts, evidence URLs, confidence, declared gaps) |
| ℹ️ About ELTR | The methodology behind the reconstruction |

## Architecture

- **Framework:** Streamlit (Databricks Apps)
- **Auth:** the app's built-in service principal (no credentials in code)
- **Data access:** Databricks SDK statement execution against the serverless SQL
  warehouse `db585e0c39b6753f`, with `StatementParameterListItem` parameters —
  all queries are parameterized and **read-only**
- **Genie:** space `01f1d53cbc42176ca9180944796f7cfd` via the SDK Genie API; if the
  app service principal lacks access, the deterministic ELTR engine answers instead

## Files

- `app.py` — the application
- `app.yaml` — app command + environment (catalog, schema, warehouse, Genie space)
- `requirements.txt` — dependencies
- `.streamlit/config.toml` — dark theme

## Deploy

```bash
databricks apps get echo-x --output JSON            # confirm RUNNING
databricks apps deploy echo-x \
  --source-code-path /Workspace/Users/<user>/echo-x-app --output JSON
```

If the app is `STOPPED`, run `databricks apps start echo-x --timeout 20m` first.

## Design guarantees

- Never writes to the historical tables — display only
- Every metric is computed live from Gold; nothing is hardcoded
- Documented vs inferred relationships are always visually distinguished
- Contradictions show both sides; unresolved decisions show declared gaps
- No fabricated facts: insufficient evidence produces an explicit statement, not a guess