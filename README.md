# 🛰️ ECHO-X
### Temporal Intelligence & Forgotten History Reconstruction Engine

> **Connecting fragmented memories to reconstruct historical truth.**

[![Databricks](https://img.shields.io/badge/Built%20on-Databricks-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/Engine-PySpark-E25A1C)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00BCE4)](https://delta.io/)
[![Streamlit](https://img.shields.io/badge/App-Streamlit-FF4B4B)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](./LICENSE.md)

**Live application:** https://echo-x-7474645617938118.aws.databricksapps.com *(workspace SSO sign-in required)*

---

## 💡 What is ECHO-X?

ECHO-X is an end-to-end **data engineering + AI system on Databricks** that reconstructs the
forgotten history of a software project from its fragmented GitHub records — issues, pull
requests, and releases. Instead of retrieving documents, it **reconstructs history**: structured
events, entities, timelines, a knowledge graph, engineering decisions, contradictions, and
evidence-linked investigations — then answers natural-language questions about *why* things
happened, with a URL to prove every claim.

Its defining discipline is **ELTR (Evidence-Linked Temporal Reconstruction)**:

- every fact is **documented** (traceable to a GitHub URL) or **labeled inference** (confidence-scored)
- contradictions keep **both sides** — they are findings, not bugs
- what cannot be reconstructed is declared as an explicit **gap**
- no timestamp is ever fabricated — every event time equals a real source-record time

## 🧩 The problem it solves

Ask a normal RAG chat “*What happened before release 0.26.0?*” or “*Why was Click vendored?*” and
you get plausible text with fabricated dates, silent contradiction resolution, and zero
provenance. ECHO-X answers from a governed temporal knowledge graph where multi-hop and
time-relative questions become **graph traversal + timestamp arithmetic** — and where
“insufficient evidence” is an acceptable answer.

## ✨ Key features

| Feature | What it does |
| --- | --- |
| 🥇 Medallion pipeline | Bronze → Silver → 15 Gold tables in Unity Catalog (idempotent MERGE ingestion) |
| 📅 Event & entity extraction | 232 events (10 types, real timestamps only) · 726 entities from 598 records |
| ⏱️ Temporal intelligence | timelines, bug→fix→release chains, technology evolution, release-window analysis |
| 🕸️ Knowledge graph | 958 nodes · 1,618 edges, every edge labeled **documented** or **inferred** + confidence |
| 🧭 Decision reconstruction | 19 decisions with triggers, implementation releases, and outcomes |
| ⚠️ Contradiction detection | 2 real findings — both sides + evidence URLs preserved (incl. a genuine repo org migration) |
| 🔎 ELTR investigations | 19 investigations with verified facts, labeled inferences, confidence scores, declared gaps |
| 🤖 Genie Investigation Assistant | narrative answers grounded in the Gold tables, with the evidence SQL shown |
| 🖥️ Deployed app | read-only Streamlit app (4 modules) on Databricks Apps with service-principal auth |
| 📊 Rigorous evaluation | 11 data-quality gates + 12-question automated benchmark suite (deterministic SQL scoring) |

## 🏗️ Architecture

```mermaid
flowchart TB
    GH["GitHub REST API<br/>tiangolo/typer (→ fastapi/typer)<br/>issues · PRs · releases"]
    subgraph LH["Databricks Lakehouse — Unity Catalog · Delta"]
        B["BRONZE — 3 raw tables<br/>MERGE-idempotent"] --> S["SILVER<br/>silver_github_records · 598"]
        S --> G["GOLD — 15 tables<br/>events · entities · timeline · chains<br/>nodes/edges · decisions · contradictions<br/>investigations · evaluation · benchmarks"]
    end
    G --> GN["Genie Agent<br/>evidence-grounded Q&A"]
    G --> APP["echo-x app (Streamlit)<br/>Dashboard · Timeline · Graph · Assistant"]
    GN -.->|Genie API + fallback| APP
    U["User (SSO)"] --> APP
    style B fill:#9e6a03,color:#fff
    style S fill:#8b949e,color:#fff
    style G fill:#58a6ff,color:#0e1117
    style APP fill:#3fb950,color:#0e1117
    style GN fill:#bc8cff,color:#0e1117
```

Full diagrams (medallion, ELTR, knowledge-graph, application): [`docs/ARCHITECTURE_DIAGRAMS.md`](./docs/ARCHITECTURE_DIAGRAMS.md)

## 🧰 Technology stack

| Layer | Technology |
| --- | --- |
| Platform | Databricks (serverless compute, Unity Catalog governance) |
| Processing | PySpark (DataFrame API, window functions, MERGE) |
| Storage | Delta Lake tables in Unity Catalog (18 tables) |
| Ingestion | GitHub REST API v3 (pagination + rate-limit handling) |
| AI layer | Databricks Genie agent (attached Gold tables, evidence-grounding instructions) |
| Application | Streamlit on Databricks Apps · Plotly · streamlit-agraph · Databricks SDK |
| Auth | App service principal (OAuth) — read-only `SELECT` grants, no secrets in code |

## 📈 Evaluation results

> Results from the **current dataset (typer GitHub records, 598 Silver rows)** and the **reported
> evaluation run `run_20261101_060656`** — a methodology validation, not a universal guarantee.

| Dimension | Result |
| --- | --- |
| Advanced benchmark questions | **12 / 12 passed (100%)** — knowledge graph · temporal · decision · contradiction · evidence synthesis |
| Correctness / evidence coverage / traceability | 100% · 100% · 100% |
| Temporal / relationship / contradiction handling | 100% · 100% · 100% |
| **Unsupported (hallucinated) claims** | **0.0%** |
| Data-quality gates | 11 / 11 at target (0 fabricated timestamps, 0 chronology violations) |

Full report: [`evaluation/BENCHMARK_RESULTS.md`](./evaluation/BENCHMARK_RESULTS.md) ·
[`docs/RESULTS_AND_EVALUATION.md`](./docs/RESULTS_AND_EVALUATION.md)

## 🖥️ The application (4 modules)

1. **🛰️ Intelligence Dashboard** — live KPIs and 7 chart sections, all computed from Gold at view time
2. **📜 Timeline Explorer** — filterable chronological events, each expandable to its GitHub source + Silver record
3. **🕸️ Knowledge Graph Explorer** — interactive graph; green edges documented, amber inferred; bug→fix→release chains
4. **🔎 Investigation Assistant** — Genie narrative answers (with evidence SQL) or the deterministic ELTR engine

### Screenshots

Screenshots live in [`screenshots/`](./screenshots/README.md). *(Placeholders below are
linked to filenames to add — see the screenshot checklist.)*

| Dashboard | Knowledge Graph |
| --- | --- |
| ![Dashboard](screenshots/01_dashboard.png) | ![Graph](screenshots/03_knowledge_graph.png) |

## 🚀 Quick start

The full reproduction guide (prerequisites → ingestion → Gold → Genie → benchmarks → deploy,
with verification checklist and troubleshooting) is in
[`docs/EXECUTION_GUIDE.md`](./docs/EXECUTION_GUIDE.md). Summary:

1. Run the pipeline notebook top-to-bottom in a Databricks workspace (Bronze is idempotent; Silver is derived).
2. Create the Genie space (instructions embedded in the notebook).
3. Deploy the app: `databricks apps deploy echo-x --source-code-path <repo>/apps/echo-x`
4. Grant the app's service principal read-only access to the schema (`GRANT SELECT ON SCHEMA`).

## 📂 Repository structure

```text
ECHO-X/
├── README.md                  ← you are here
├── LICENSE.md                 ← MIT
├── .gitignore
├── apps/echo-x/               ← deployed Streamlit app (Databricks Apps git source)
├── notebooks/                 ← pipeline notebook (see README in folder)
├── docs/                      ← full documentation set
│   ├── PROJECT_REPORT.md      ← 20-section project report
│   ├── TECHNICAL_AUDIT.md     ← component-by-component audit
│   ├── ARCHITECTURE_DIAGRAMS.md
│   ├── EXECUTION_GUIDE.md
│   ├── RESULTS_AND_EVALUATION.md
│   ├── LIMITATIONS_AND_ROADMAP.md
│   └── INTERVIEW_PREPARATION.md
├── screenshots/               ← app screenshots (see checklist)
└── evaluation/                ← benchmark artifacts
```

## ⚠️ Limitations & future scope (honest by design)

- Anonymous GitHub API caps issues/PRs at 5 pages (recent history; releases are complete 2019→2026)
- Rule-based extraction — subtle discussions without expected keywords are not extracted (reported honestly as 0)
- “Rejected” decision status not derivable from current Silver schema (declared, not invented)
- Genie tier needs a one-time space permission; the app falls back to a deterministic engine otherwise

Full list + prioritized roadmap: [`docs/LIMITATIONS_AND_ROADMAP.md`](./docs/LIMITATIONS_AND_ROADMAP.md)

## 👤 Author

**Vijayanandhini** — built as a capstone data-engineering portfolio project on Databricks.
Built to be explored: every claimed number above is re-verifiable from the repo's notebook,
the benchmark suite, and the deployed application.

---

*If a fact above has no URL behind it, ECHO-X wouldn't claim it.*