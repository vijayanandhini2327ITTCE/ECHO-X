# screenshots/ — Application Screenshots

Real screenshots of the deployed ECHO-X application
(https://echo-x-7474645617938118.aws.databricksapps.com — workspace SSO required).

> **Integrity note:** ECHO-X never fabricates data — and this repo never fabricates UI
> screenshots. Every image below must be an actual capture from the running application.
> This folder contains no images until they are captured by a signed-in user; the
> checklist below tells you exactly what to capture.

## Required screenshots (five — the new six-page premium UI)

| # | Page (sidebar) | Filename | What must be visible |
| --- | --- | --- | --- |
| 1 | 🛰️ Command Center | `01-command-center.png` | The five KPI tiles with real numbers (598 records · 232 events · 726 entities · 1,618 edges · 19 decisions) and at least the first two chart sections fully drawn |
| 2 | ⏱️ Temporal Intelligence | `02-temporal-intelligence.png` | The zoomable lane timeline with events plotted, the tone legend visible, and one event expanded to show its GitHub source + Silver record detail |
| 3 | 🕸️ Knowledge Graph Explorer | `03-knowledge-graph.png` | The interactive graph with nodes + edges rendered and the legend showing green = Documented / amber = Inferred; optionally select a hub node so its evidence panel is open |
| 4 | 🔎 Investigation Studio | `04-investigation-studio.png` | A genuine answered question with its grounded evidence — ask “Why was Click vendoring introduced?” — showing the confidence score and evidence URLs. Capture whichever engine answered (Genie or the deterministic ELTR fallback) — both are real behavior, but note which one in the commit message |
| 5 | 📊 Evaluation & Quality | `05-evaluation-quality.png` | The recorded benchmark run (12/12 passed), the quality-gates grid (11/11 at target) and the evaluation history section |

## Capture procedure (exact)

1. Sign in to the app URL above (Databricks workspace SSO — the app is **not** publicly accessible).
2. Use one browser window for all five captures at a consistent size (recommended:
   maximized window at ~1600×1000, or full-screen F11). Same window, same zoom (100%).
3. Hide browser clutter first: hide the bookmarks bar (Ctrl+Shift+B) and close dev tools.
4. For each page in the table above, click it in the left sidebar and **wait until content
   has finished loading** — no skeleton shimmer, no spinner, no error banners. Only
   meaningful real data qualifies; do not capture empty states, loading states, or errors.
5. Verify the numbers shown match the audited values before capturing (the KPI tiles make
   this easy on page 1: 598 / 232 / 726 / 1,618 / 19).
6. Save as PNG in this folder using the **exact filenames** above. The root `README.md`
   embeds these five filenames, so images render there automatically once committed.
7. Commit and push (e.g. `Add real application screenshots`), then check the README renders
   all five images.

## Why these five

They demonstrate the five pillars a technical reviewer looks for: live-data dashboard,
temporal reasoning, graph + evidence provenance, natural-language investigation with
grounded evidence, and a rigorous evaluation story.
