# screenshots/ — Application Screenshots

Real screenshots of the deployed ECHO-X application
(https://echo-x-7474645617938118.aws.databricksapps.com — workspace SSO required).

> **Integrity note:** ECHO-X never fabricates data — and this repo never fabricates UI
> screenshots. Every image below must be an actual capture from the running application.

## Checklist

| # | Screenshot | Filename | How to capture |
| --- | --- | --- | --- |
| 1 | 🛰️ Intelligence Dashboard (KPI row + charts) | `01_dashboard.png` | Open the app → Intelligence Dashboard → capture full page (scroll) |
| 2 | 📜 Timeline Explorer (filters + event expanders) | `02_timeline.png` | Timeline Explorer → apply a filter → expand one event showing its GitHub source |
| 3 | 🕸️ Knowledge Graph (documented vs inferred edges) | `03_knowledge_graph.png` | Knowledge Graph Explorer → select a hub node → ensure green + amber edges visible |
| 4 | 🔎 Investigation Assistant (evidence-grounded answer) | `04_assistant.png` | Investigation Assistant → ask “Why was Click vendoring introduced?” → capture answer incl. confidence + evidence links |
| 5 | 📊 Benchmark results | `05_benchmark.png` | Dashboard → “Benchmark performance (latest run)” bar chart + caption (or the notebook cell 33 report chart) |

## Steps

1. Sign in to the app URL.
2. Capture each module (full-page screenshots; dark theme is the default look).
3. Save PNGs in this folder with the exact filenames above — the root `README.md` already
   links to `01_dashboard.png` and `03_knowledge_graph.png`, and more can be added.
4. Commit and push.

After adding the images, no README edits are needed for items 1 and 3 — the links resolve
automatically.
