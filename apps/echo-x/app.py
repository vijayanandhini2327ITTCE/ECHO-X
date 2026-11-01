# ECHO-X — Temporal Intelligence & Forgotten History Reconstruction Engine
# Interactive research-intelligence app over the ECHO-X Gold layer.
# STRICTLY READ-ONLY: this app never writes to the historical data tables.
import os
import re
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
from streamlit_agraph import Config as AGraphConfig
from streamlit_agraph import Edge as AEdge
from streamlit_agraph import Node as ANode
from streamlit_agraph import agraph

st.set_page_config(
    page_title="ECHO-X Intelligence",
    page_icon="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath fill='%2322d3ee' d='M12 2l2.4 7.6L22 12l-7.6 2.4L12 22l-2.4-7.6L2 12l7.6-2.4z'/%3E%3C/svg%3E",
    layout="wide")

CATALOG = os.environ.get("ECHO_X_CATALOG", "learn_get_started_de_vijayanandhini")
SCHEMA = os.environ.get("ECHO_X_SCHEMA", "echo_x")
WAREHOUSE_ID = os.environ.get("ECHO_X_WAREHOUSE_ID", "db585e0c39b6753f")
GENIE_SPACE_ID = os.environ.get("ECHO_X_GENIE_SPACE_ID",
                                "01f1d53cbc42176ca9180944796f7cfd")
T = f"{CATALOG}.{SCHEMA}"  # table prefix

C = {"accent": "#22d3ee", "cyan": "#22d3ee", "violet": "#a78bfa",
     "ok": "#34d399", "warn": "#f6c453", "bad": "#f0716c", "muted": "#93a1b8",
     "dev": "#a78bfa", "event": "#60a5fa", "tech": "#34d399",
     "record": "#93a1b8", "release": "#22d3ee", "blue": "#60a5fa"}

st.markdown("""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
:root{
  --ex-bg:#060a13; --ex-surface:#0c1322; --ex-surface2:#101a2c; --ex-surface3:#0a101d;
  --ex-border:rgba(148,163,184,.13); --ex-border-strong:rgba(148,163,184,.28);
  --ex-text:#e8eef7; --ex-muted:#93a1b8; --ex-faint:#5f6e86;
  --ex-cyan:#22d3ee; --ex-cyan-soft:rgba(34,211,238,.09); --ex-cyan-line:rgba(34,211,238,.38);
  --ex-violet:#a78bfa; --ex-violet-soft:rgba(167,139,250,.10);
  --ex-emerald:#34d399; --ex-emerald-soft:rgba(52,211,153,.10);
  --ex-amber:#f6c453; --ex-amber-soft:rgba(246,196,83,.10);
  --ex-red:#f0716c; --ex-red-soft:rgba(240,113,108,.10);
  --ex-radius:12px;
}
html, body, .stApp, [data-testid="stAppViewContainer"]{font-family:'Inter',-apple-system,'Segoe UI',sans-serif;}
.stApp, [data-testid="stAppViewContainer"]{
  background:radial-gradient(1100px 700px at 85% -12%, rgba(34,211,238,.055), transparent 55%),
             radial-gradient(900px 600px at -10% 110%, rgba(167,139,250,.05), transparent 55%),
             var(--ex-bg); color:var(--ex-text);}
[data-testid="stHeader"]{background:transparent;}
[data-testid="stDecoration"]{display:none;}
footer, [data-testid="stStatusWidget"]{visibility:hidden;}
.block-container{padding-top:1.6rem; padding-bottom:3rem; max-width:1460px;}
h1,h2,h3{font-weight:700; letter-spacing:-.02em; color:var(--ex-text);}
p, li{color:var(--ex-muted); line-height:1.55;}
a{color:var(--ex-cyan);}

/* ---- sidebar shell ---- */
[data-testid="stSidebar"]{background:linear-gradient(180deg,#080d18,#070b14); border-right:1px solid var(--ex-border);}
[data-testid="stSidebarContent"]{padding-top:1.1rem;}
section[data-testid="stSidebar"] .block-container{padding-top:0 !important;}

/* ---- premium nav (radio-based, keyboard accessible) ---- */
[data-testid="stSidebar"] [data-testid="stRadio"]{gap:2px;}
[data-testid="stSidebar"] [data-testid="stRadio"] label{
  display:flex; align-items:center; width:100%; padding:9px 12px; margin:1px 0;
  border-radius:9px; border:1px solid transparent; background:transparent;
  color:var(--ex-muted); font-size:.855rem; font-weight:500; letter-spacing:.01em;
  transition:background .15s ease, color .15s ease, border-color .15s ease; cursor:pointer;}
[data-testid="stSidebar"] [data-testid="stRadio"] label:hover{background:rgba(148,163,184,.06); color:var(--ex-text);}
[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked){
  background:var(--ex-cyan-soft); border-color:var(--ex-cyan-line); color:#dffaff;}
[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:focus-visible){outline:1px solid var(--ex-cyan); outline-offset:1px;}
[data-testid="stSidebar"] [data-testid="stRadio"] label input{position:absolute; opacity:0; width:1px; height:1px;}

/* ---- brand ---- */
.ex-brand{display:flex; align-items:center; gap:11px; padding:4px 6px 12px 6px;}
.ex-brand-mark{flex:0 0 auto; filter:drop-shadow(0 0 12px rgba(34,211,238,.45));}
.ex-brand-name{font-size:1.02rem; font-weight:800; letter-spacing:.06em; color:var(--ex-text);}
.ex-brand-sub{font-size:.62rem; font-weight:500; letter-spacing:.14em; text-transform:uppercase; color:var(--ex-cyan); opacity:.9;}
.ex-side-divider{height:1px; margin:10px 2px 14px 2px; background:linear-gradient(90deg, transparent, var(--ex-border-strong), transparent);}

/* ---- page header ---- */
.ex-pagehead{display:flex; align-items:flex-start; gap:14px; margin:2px 0 20px 0;}
.ex-pagehead-icon{flex:0 0 auto; display:flex; align-items:center; justify-content:center; width:42px; height:42px;
  border-radius:11px; background:var(--ex-cyan-soft); border:1px solid var(--ex-cyan-line); color:var(--ex-cyan);}
.ex-pagehead h1{font-size:1.42rem; font-weight:700; letter-spacing:-.02em; margin:0; color:var(--ex-text);}
.ex-pagehead p{font-size:.82rem; color:var(--ex-muted); margin:3px 0 0 0;}

/* ---- cards & panels ---- */
.ex-card{background:linear-gradient(180deg, var(--ex-surface), var(--ex-surface3)); border:1px solid var(--ex-border);
  border-radius:var(--ex-radius); padding:16px 18px; box-shadow:0 1px 0 rgba(255,255,255,.02) inset, 0 8px 24px rgba(0,0,0,.35);}
.ex-card-title{display:flex; align-items:center; gap:8px; font-size:.86rem; font-weight:600; color:var(--ex-text);
  margin-bottom:10px; letter-spacing:.01em;}
.ex-panel{border-left:2px solid var(--ex-border-strong); padding:2px 0 2px 12px; margin:6px 0;}
.ex-kicker{font-size:.68rem; font-weight:600; letter-spacing:.14em; text-transform:uppercase; color:var(--ex-faint); margin:0 0 2px 0;}

/* ---- KPI tiles ---- */
.ex-kpi{background:linear-gradient(180deg, var(--ex-surface), var(--ex-surface3)); border:1px solid var(--ex-border);
  border-radius:var(--ex-radius); padding:13px 15px; position:relative; overflow:hidden;}
.ex-kpi::before{content:''; position:absolute; inset:0 auto 0 0; width:2px;
  background:linear-gradient(180deg, var(--ex-cyan), rgba(34,211,238,0));}
.ex-kpi .ex-kpi-label{font-size:.66rem; font-weight:600; letter-spacing:.12em; text-transform:uppercase; color:var(--ex-faint);}
.ex-kpi .ex-kpi-value{font-size:1.72rem; font-weight:800; letter-spacing:-.02em; color:var(--ex-text); line-height:1.25; margin-top:2px;}
.ex-kpi .ex-kpi-hint{font-size:.68rem; color:var(--ex-faint); font-family:ui-monospace,Menlo,monospace;}

/* ---- badges & chips ---- */
.ex-badge{display:inline-flex; align-items:center; gap:5px; padding:2px 9px; border-radius:999px;
  font-size:.68rem; font-weight:600; letter-spacing:.05em; border:1px solid; white-space:nowrap;}
.ex-badge--cyan{color:var(--ex-cyan); background:var(--ex-cyan-soft); border-color:var(--ex-cyan-line);}
.ex-badge--ok{color:var(--ex-emerald); background:var(--ex-emerald-soft); border-color:rgba(52,211,153,.35);}
.ex-badge--warn{color:var(--ex-amber); background:var(--ex-amber-soft); border-color:rgba(246,196,83,.35);}
.ex-badge--bad{color:var(--ex-red); background:var(--ex-red-soft); border-color:rgba(240,113,108,.35);}
.ex-badge--violet{color:var(--ex-violet); background:var(--ex-violet-soft); border-color:rgba(167,139,250,.35);}
.ex-badge--muted{color:var(--ex-muted); background:rgba(148,163,184,.07); border-color:var(--ex-border-strong);}
.ex-chip{display:inline-block; margin:2px 4px 2px 0; padding:3px 10px; border-radius:7px; border:1px solid var(--ex-border);
  background:var(--ex-surface2); font-size:.72rem; color:var(--ex-muted);}

/* ---- states ---- */
.ex-empty{border:1px dashed var(--ex-border-strong); border-radius:var(--ex-radius); padding:30px 20px;
  text-align:center; color:var(--ex-muted); background:rgba(148,163,184,.03);}
.ex-empty .ex-empty-title{font-weight:600; color:var(--ex-text); font-size:.9rem; margin-top:8px;}
.ex-empty p{font-size:.78rem; margin:4px 0 0 0;}
.ex-error{border:1px solid rgba(240,113,108,.4); border-left:3px solid var(--ex-red); border-radius:var(--ex-radius);
  padding:14px 16px; background:var(--ex-red-soft); color:#ffd9d7; font-size:.85rem;}
@keyframes ex-shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
.ex-skeleton{border-radius:9px; background:linear-gradient(90deg, #0b1120 25%, #14203a 50%, #0b1120 75%);
  background-size:200% 100%; animation:ex-shimmer 1.5s linear infinite;}

/* ---- widget overrides ---- */
.stButton > button, [data-testid="stFormSubmitButton"] > button{
  background:var(--ex-surface2); color:var(--ex-text); border:1px solid var(--ex-border-strong);
  border-radius:9px; font-weight:600; font-size:.84rem; padding:.42rem 1rem; letter-spacing:.01em;
  transition:all .15s ease;}
.stButton > button:hover{border-color:var(--ex-cyan); color:var(--ex-cyan); box-shadow:0 0 0 1px rgba(34,211,238,.15);}
.stButton > button[kind="primary"], [data-testid="stFormSubmitButton"] > button[kind="primary"],
.stButton > button[data-testid="stBaseButton-primary"]{
  background:linear-gradient(180deg, rgba(34,211,238,.24), rgba(34,211,238,.10));
  border:1px solid rgba(34,211,238,.5); color:#eafcff;}
.stButton > button[kind="primary"]:hover, .stButton > button[data-testid="stBaseButton-primary"]:hover{
  background:linear-gradient(180deg, rgba(34,211,238,.34), rgba(34,211,238,.16)); color:#fff; border-color:var(--ex-cyan);}
[data-testid="stTextInput"] input, [data-testid="stTextArea"] textarea{
  background:var(--ex-surface); border:1px solid var(--ex-border-strong); border-radius:9px;
  color:var(--ex-text); font-size:.86rem;}
[data-testid="stTextInput"] input:focus, [data-testid="stTextArea"] textarea:focus{border-color:var(--ex-cyan); box-shadow:0 0 0 2px rgba(34,211,238,.12);}
[data-testid="stSelectbox"] > div > div, [data-testid="stMultiselect"] > div > div{
  background:var(--ex-surface); border-color:var(--ex-border-strong); border-radius:9px;}
[data-testid="stExpander"]{background:var(--ex-surface3); border:1px solid var(--ex-border); border-radius:11px; overflow:hidden;}
[data-testid="stExpander"] summary{font-size:.84rem; font-weight:500; color:var(--ex-text);}
[data-testid="stExpander"] summary:hover{background:rgba(148,163,184,.05);}
[data-testid="stMetric"]{background:linear-gradient(180deg, var(--ex-surface), var(--ex-surface3));
  border:1px solid var(--ex-border); border-radius:var(--ex-radius); padding:13px 16px;}
[data-testid="stMetricLabel"] p{font-size:.66rem; font-weight:600; letter-spacing:.11em; text-transform:uppercase; color:var(--ex-faint);}
[data-testid="stMetricValue"]{font-weight:800; font-size:1.55rem; letter-spacing:-.02em; color:var(--ex-text);}
[data-testid="stDataFrame"]{border:1px solid var(--ex-border); border-radius:11px; overflow:hidden;}
hr{border-color:var(--ex-border);}

/* ---- scrollbars & misc ---- */
::-webkit-scrollbar{width:9px; height:9px;}
::-webkit-scrollbar-thumb{background:#1b2740; border-radius:5px;}
::-webkit-scrollbar-track{background:transparent;}
[data-testid="stSpinner"] p{color:var(--ex-muted); font-size:.82rem;}
[data-baseweb="popover"]{background:var(--ex-surface2) !important; border:1px solid var(--ex-border-strong) !important;}
[data-baseweb="tooltip"]{background:#182338 !important; color:var(--ex-text) !important; border:1px solid var(--ex-border-strong);}
[data-testid="stTooltipContent"]{color:var(--ex-text); font-size:.75rem;}

/* ---- st.navigation sidebar links ---- */
[data-testid="stSidebarNav"]{gap:2px;}
[data-testid="stSidebarNavItems"]{gap:2px;}
[data-testid="stSidebarNavLink"]{border-radius:9px; padding:8px 10px; margin:1px 0;
  font-size:.86rem; font-weight:500; color:var(--ex-muted);}
[data-testid="stSidebarNavLink"]:hover{background:rgba(148,163,184,.06); color:var(--ex-text);}
[data-testid="stSidebarNavLink"][aria-current="page"]{
  background:var(--ex-cyan-soft); color:#dffaff; box-shadow:inset 2px 0 0 var(--ex-cyan);}
[data-testid="stSidebarNavLink"] span{color:inherit;}
</style>""", unsafe_allow_html=True)


# --------------------------------------------------------------------------- #
# Data access layer (read-only, parameterized SQL via statement execution)
# --------------------------------------------------------------------------- #
@st.cache_resource
def ws_client() -> WorkspaceClient:
    """App service-principal auth (injected by Databricks Apps)."""
    return WorkspaceClient()


def run_sql(sql, params=None, poll_rounds=12):
    """Execute a read-only query. params: list of (name, value, type)."""
    w = ws_client()
    plist = [StatementParameterListItem(name=n, value=str(v), type=t)
             for (n, v, t) in (params or [])]
    resp = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=sql, parameters=plist,
        wait_timeout="50s")
    n = 0
    while resp.status.state.value == "PENDING" and n < poll_rounds:
        time.sleep(5)
        n += 1
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.error is not None:
        raise RuntimeError(f"Query failed: {resp.status.error.message}")
    if not resp.manifest:
        return []
    cols = [c.name for c in resp.manifest.schema.columns]
    rows = (resp.result.data_array or []) if resp.result is not None else []
    return [dict(zip(cols, r)) for r in rows]


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _i(v):
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _mdlink(url, label=None):
    if not url:
        return "—"
    u = str(url)
    shown = label or u.replace("https://", "").replace("http://", "")
    return f"[{shown}]({u})"


# --------------------------------------------------------------------------- #
# Additional queries — Evaluation & Quality, Command Center (read-only)
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=300, show_spinner=False)
def events_over_time():
    return run_sql(f"""SELECT date_trunc('month', event_time) AS mon, COUNT(*) AS n
                     FROM {T}.gold_events GROUP BY 1 ORDER BY 1""")


@st.cache_data(ttl=300, show_spinner=False)
def investigation_confidences():
    return run_sql(f"""SELECT overall_confidence FROM {T}.gold_investigations""")


@st.cache_data(ttl=300, show_spinner=False)
def recent_events(limit=6):
    return run_sql(f"""SELECT event_id, event_type, event_title, event_time, source_url
                     FROM {T}.gold_events ORDER BY event_time DESC LIMIT {limit}""")


@st.cache_data(ttl=300, show_spinner=False)
def recent_investigations(limit=6):
    return run_sql(f"""SELECT investigation_id, decision_title, decision_date,
                              decision_status, overall_confidence, reconstruction_class
                     FROM {T}.gold_investigations
                     ORDER BY decision_date DESC LIMIT {limit}""")


@st.cache_data(ttl=300, show_spinner=False)
def quality_metrics():
    return run_sql(f"""SELECT metric_name, metric_value, metric_meaning
                     FROM {T}.gold_evaluation_metrics ORDER BY metric_name""")


@st.cache_data(ttl=300, show_spinner=False)
def benchmark_history():
    return run_sql(f"""SELECT run_id, executed_at, total_benchmarks, passed, failed,
                              accuracy_pct, unsupported_claims_found_pct
                     FROM {T}.gold_benchmark_summary ORDER BY executed_at DESC""")


@st.cache_data(ttl=300, show_spinner=False)
def benchmark_results_for(run_id):
    return run_sql(f"""SELECT question_id, category, answer_summary, checks_passed,
                              checks_total, score, passed, failure_explanation,
                              answer_correctness, evidence_coverage, source_traceability,
                              temporal_accuracy, relationship_accuracy,
                              unsupported_claim_rate, contradiction_handling
                     FROM {T}.gold_benchmark_results WHERE run_id = :rid
                     ORDER BY question_id""",
                   [("rid", run_id, "STRING")])


@st.cache_data(ttl=300, show_spinner=False)
def pipeline_counts():
    return run_sql(f"""SELECT (SELECT COUNT(*) FROM {T}.bronze_github_issues) AS issues,
                              (SELECT COUNT(*) FROM {T}.bronze_github_pull_requests) AS prs,
                              (SELECT COUNT(*) FROM {T}.bronze_github_releases) AS releases,
                              (SELECT COUNT(*) FROM {T}.silver_github_records) AS silver""")


# --------------------------------------------------------------------------- #
# Design system — iconography (Lucide-style inline SVG) and UI primitives
# --------------------------------------------------------------------------- #
ICONS = {
    "orbit": '<circle cx="12" cy="12" r="3"/><circle cx="19" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><path d="M10.4 21.9a10 10 0 0 0 9.941-15.416"/><path d="M13.5 2.1a10 10 0 0 0-9.841 15.416"/>',
    "clock": '<circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>',
    "share": '<circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" x2="15.42" y1="13.51" y2="8.49"/><line x1="15.41" x2="8.59" y1="18.51" y2="13.49"/>',
    "search": '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>',
    "gauge": '<path d="m12 14 4-4"/><path d="M3.34 19a10 10 0 1 1 17.32 0"/>',
    "info": '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>',
    "check": '<circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>',
    "alert": '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"/><path d="M12 9v4"/><path d="M12 17h.01"/>',
    "git": '<line x1="6" x2="6" y1="3" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/>',
    "external": '<path d="M15 3h6v6"/><path d="M10 14 21 3"/><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>',
    "database": '<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/>',
    "shield": '<path d="M20 13c0 5-3.5 7.5-7.66 8.95a1 1 0 0 1-.67-.01C7.5 20.5 4 18 4 13V6a1 1 0 0 1 1-1c2 0 4.5-1.2 6.24-2.72a1.17 1.17 0 0 1 1.52 0C14.51 3.81 17 5 19 5a1 1 0 0 1 1 1z"/><path d="m9 12 2 2 4-4"/>',
    "sparkle": '<path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z"/>',
    "layers": '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22 17.65-9.17 4.16a2 2 0 0 1-1.66 0L2 17.65"/><path d="m22 12.65-9.17 4.16a2 2 0 0 1-1.66 0L2 12.65"/>',
    "users": '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    "file": '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/><path d="M14 2v4a2 2 0 0 0 2 2h4"/><path d="M10 9H8"/><path d="M16 13H8"/><path d="M16 17H8"/>',
    "bulb": '<path d="M15 14c.2-1 .7-1.7 1.5-2.5 1-.9 1.5-2.2 1.5-3.5A6 6 0 0 0 6 8c0 1 .2 2.2 1.5 3.5.7.7 1.3 1.5 1.5 2.5"/><path d="M9 18h6"/><path d="M10 22h4"/>',
    "x": '<circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/>',
}


def svg(name, size=18, color="currentColor"):
    return (f'<svg width="{size}" height="{size}" viewBox="0 0 24 24" fill="none" '
            f'stroke="{color}" stroke-width="2" stroke-linecap="round" '
            f'stroke-linejoin="round" style="vertical-align:-3px">{ICONS[name]}</svg>')


LOGO_SVG = ('<svg width="34" height="34" viewBox="0 0 24 24" fill="none">'
            '<circle cx="12" cy="12" r="9.5" stroke="#22d3ee" stroke-opacity=".45" stroke-width="1.4"/>'
            '<circle cx="12" cy="12" r="6" stroke="#22d3ee" stroke-opacity=".75" stroke-width="1.4"/>'
            '<path d="M12 2l1.9 5.9L20 9.8l-6.1 1.9L12 17.6l-1.9-5.9L4 9.8l6.1-1.9z" fill="#22d3ee"/>'
            '</svg>')


def page_header(icon, title, subtitle):
    st.markdown(
        f'<div class="ex-pagehead"><div class="ex-pagehead-icon">{svg(icon, 21, "#22d3ee")}</div>'
        f'<div><h1>{title}</h1><p>{subtitle}</p></div></div>', unsafe_allow_html=True)


def badge(text, tone="cyan"):
    return f'<span class="ex-badge ex-badge--{tone}">{text}</span>'


def kpi_tile(label, value, hint=""):
    h = f'<div class="ex-kpi-hint">{hint}</div>' if hint else ""
    return (f'<div class="ex-kpi"><div class="ex-kpi-label">{label}</div>'
            f'<div class="ex-kpi-value">{value}</div>{h}</div>')


def empty_state(icon_name, title, message):
    return (f'<div class="ex-empty">{svg(icon_name, 26, "#5f6e86")}'
            f'<div class="ex-empty-title">{title}</div><p>{message}</p></div>')


def error_state(e):
    return (f'<div class="ex-error">{svg("alert", 16, "#f0716c")} '
            f'<b>Data could not be loaded.</b> {type(e).__name__}: {str(e)[:200]}</div>')


def skeleton(h="86px"):
    return f'<div class="ex-skeleton" style="height:{h}"></div>'


def event_type_badge(t):
    tone = {"bug_discovery": "bad", "bug_resolution": "ok", "version_release": "cyan",
            "feature_introduction": "violet", "deprecation": "warn",
            "technology_migration": "violet", "security_incident": "bad",
            "performance_improvement": "cyan", "architecture_modification": "violet",
            "design_discussion": "muted", "tech_mention": "muted"}.get(t, "muted")
    return badge(t.replace("_", " "), tone)


# --------------------------------------------------------------------------- #
# Module 1 — Intelligence Command Center
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=300, show_spinner=False)
def kpis():
    rows = run_sql(f"""SELECT
      (SELECT COUNT(*) FROM {T}.silver_github_records) AS records,
      (SELECT COUNT(*) FROM {T}.gold_events) AS events,
      (SELECT COUNT(*) FROM {T}.gold_entities) AS entities,
      (SELECT COUNT(*) FROM {T}.gold_nodes) AS nodes,
      (SELECT COUNT(*) FROM {T}.gold_edges) AS edges,
      (SELECT COUNT(*) FROM {T}.gold_decisions) AS decisions,
      (SELECT COUNT(*) FROM {T}.contradiction_records) AS contradictions,
      (SELECT COUNT(*) FROM {T}.gold_investigations) AS investigations,
      (SELECT ROUND(AVG(overall_confidence), 3) FROM {T}.gold_investigations) AS avg_confidence,
      (SELECT accuracy_pct FROM {T}.gold_benchmark_summary
         ORDER BY executed_at DESC LIMIT 1) AS benchmark_accuracy""")
    return rows[0] if rows else {}


@st.cache_data(ttl=300, show_spinner=False)
def events_by_type():
    return run_sql(f"""SELECT event_type, COUNT(*) AS n FROM {T}.gold_events
                     GROUP BY event_type ORDER BY n DESC""")


@st.cache_data(ttl=300, show_spinner=False)
def tech_evolution():
    return run_sql(f"""SELECT normalized_name, year, mentions
                     FROM {T}.gold_technology_evolution
                     ORDER BY normalized_name, year""")


@st.cache_data(ttl=300, show_spinner=False)
def incident_stats():
    return run_sql(f"""SELECT bug_title, discovered_at, resolved_at,
                            resolution_days, source_url
                     FROM {T}.gold_incident_resolutions""")


@st.cache_data(ttl=300, show_spinner=False)
def top_developers(limit=15):
    return run_sql(f"""SELECT label, mention_count FROM {T}.gold_nodes
                     WHERE node_id LIKE 'developer:%'
                     ORDER BY CAST(mention_count AS INT) DESC LIMIT {limit}""")


@st.cache_data(ttl=300, show_spinner=False)
def decision_status():
    return run_sql(f"""SELECT decision_status, COUNT(*) AS n
                     FROM {T}.gold_decisions GROUP BY decision_status
                     ORDER BY n DESC""")


@st.cache_data(ttl=300, show_spinner=False)
def contradictions():
    return run_sql(f"""SELECT category, subject, attribute, value_a, value_b,
                            records_affected, confidence_score, evidence_urls
                     FROM {T}.contradiction_records""")


@st.cache_data(ttl=300, show_spinner=False)
def benchmark_summary():
    rows = run_sql(f"""SELECT * FROM {T}.gold_benchmark_summary
                     ORDER BY executed_at DESC LIMIT 1""")
    return rows[0] if rows else {}


def _fig(fig):
    fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                      plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e6edf3"), margin=dict(l=10, r=10, t=40, b=10))
    return fig


def page_command():
    page_header("orbit", "Intelligence Command Center",
                "Live overview of the reconstructed historical record — every metric is computed "
                "from the ECHO-X Gold tables at view time. Nothing is hardcoded.")

    try:
        ph = st.empty()
        ph.markdown("<div style='display:flex'>" +
                    "".join(f"<div style='flex:1;margin:0 6px'>{skeleton()}</div>"
                            for _ in range(4)) + "</div>", unsafe_allow_html=True)
        k = kpis()
        ph.empty()
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)
        return

    tiles = [
        ("Historical records", k.get("records"), "silver_github_records"),
        ("Extracted events", k.get("events"), "gold_events"),
        ("Entities", k.get("entities"), "gold_entities"),
        ("Graph nodes", k.get("nodes"), "gold_nodes"),
        ("Graph edges", k.get("edges"), "gold_edges"),
        ("Decisions", k.get("decisions"), "gold_decisions"),
        ("Contradictions", k.get("contradictions"), "contradiction_records"),
        ("Benchmark accuracy", f"{_f(k.get('benchmark_accuracy')):.0f}%",
         "latest gold_benchmark_summary run"),
    ]
    for row in (tiles[:4], tiles[4:]):
        cs = st.columns(4)
        for c, (label, val, hint) in zip(cs, row):
            c.markdown(kpi_tile(label, val if val is not None else "—", hint),
                       unsafe_allow_html=True)
    st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

    try:
        cols = st.columns(2)
        with cols[0]:
            st.markdown(f'<div class="ex-card-title">{svg("clock", 16, "#22d3ee")} '
                        'Historical events over time</div>', unsafe_allow_html=True)
            ot = pd.DataFrame(events_over_time())
            if not ot.empty:
                ot["n"] = pd.to_numeric(ot["n"], errors="coerce")
                ot["mon"] = pd.to_datetime(ot["mon"])
                st.plotly_chart(_fig(px.area(ot, x="mon", y="n",
                                color_discrete_sequence=[C["accent"]],
                                line_shape="spline")), use_container_width=True, key="cc_ot")
            else:
                st.markdown(empty_state("clock", "No events", "No events recorded yet."),
                            unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f'<div class="ex-card-title">{svg("sparkle", 16, "#a78bfa")} '
                        'Event distribution by type</div>', unsafe_allow_html=True)
            d = pd.DataFrame(events_by_type())
            if not d.empty:
                d["n"] = pd.to_numeric(d["n"], errors="coerce")
                st.plotly_chart(_fig(px.bar(d, x="n", y="event_type", orientation="h",
                                color_discrete_sequence=[C["violet"]])),
                                use_container_width=True, key="cc_dist")

        cols = st.columns(2)
        with cols[0]:
            st.markdown(f'<div class="ex-card-title">{svg("layers", 16, "#34d399")} '
                        'Technology evolution over time</div>', unsafe_allow_html=True)
            ev = pd.DataFrame(tech_evolution())
            if not ev.empty:
                ev["mentions"] = pd.to_numeric(ev["mentions"], errors="coerce")
                ev["year"] = pd.to_numeric(ev["year"], errors="coerce")
                totals = ev.groupby("normalized_name")["mentions"].sum().sort_values(ascending=False)
                sel = st.multiselect("Technologies", list(totals.index),
                                     default=list(totals.head(5).index), key="dash_tech")
                d2 = ev[ev["normalized_name"].isin(sel)]
                if not d2.empty:
                    st.plotly_chart(_fig(px.line(d2, x="year", y="mentions",
                                   color="normalized_name", markers=True,
                                   color_discrete_sequence=[C["accent"], C["violet"], C["ok"],
                                                            C["warn"], C["blue"], C["muted"],
                                                            C["release"], C["bad"], C["dev"],
                                                            C["record"], C["event"]])),
                                   use_container_width=True, key="cc_tech")
        with cols[1]:
            st.markdown(f'<div class="ex-card-title">{svg("users", 16, "#a78bfa")} '
                        'Developer contributions (documented authorship)</div>', unsafe_allow_html=True)
            dd = pd.DataFrame(top_developers())
            if not dd.empty:
                dd["mention_count"] = pd.to_numeric(dd["mention_count"], errors="coerce")
                st.plotly_chart(_fig(px.bar(dd, x="mention_count", y="label", orientation="h",
                               color_discrete_sequence=[C["dev"]])),
                               use_container_width=True, key="cc_dev")

        cols = st.columns(2)
        with cols[0]:
            st.markdown(f'<div class="ex-card-title">{svg("gauge", 16, "#22d3ee")} '
                        'Investigation confidence distribution</div>', unsafe_allow_html=True)
            ic = pd.DataFrame(investigation_confidences())
            if not ic.empty:
                ic["overall_confidence"] = pd.to_numeric(ic["overall_confidence"], errors="coerce")
                st.plotly_chart(_fig(px.histogram(ic, x="overall_confidence", nbins=8,
                               color_discrete_sequence=[C["accent"]])),
                               use_container_width=True, key="cc_conf")
        with cols[1]:
            st.markdown(f'<div class="ex-card-title">{svg("sparkle", 16, "#34d399")} '
                        'Decision status</div>', unsafe_allow_html=True)
            ds = pd.DataFrame(decision_status())
            if not ds.empty:
                ds["n"] = pd.to_numeric(ds["n"], errors="coerce")
                st.plotly_chart(_fig(px.pie(ds, names="decision_status", values="n", hole=0.6,
                               color_discrete_sequence=[C["ok"], C["warn"], C["bad"]])),
                               use_container_width=True, key="cc_dec")
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

    try:
        bs = benchmark_summary()
        if bs:
            st.markdown(f'<div class="ex-card-title">{svg("shield", 16, "#34d399")} '
                        'Benchmark performance — latest run</div>', unsafe_allow_html=True)
            dims = [("Correctness", bs.get("answer_correctness_pct")),
                    ("Evidence coverage", bs.get("evidence_coverage_pct")),
                    ("Traceability", bs.get("source_traceability_pct")),
                    ("Temporal accuracy", bs.get("temporal_accuracy_pct")),
                    ("Relationship accuracy", bs.get("relationship_accuracy_pct")),
                    ("Contradiction handling", bs.get("contradiction_handling_pct"))]
            bd = pd.DataFrame(dims, columns=["dimension", "pct"])
            bd["pct"] = pd.to_numeric(bd["pct"], errors="coerce")
            st.plotly_chart(_fig(px.bar(bd, x="pct", y="dimension", orientation="h",
                           range_x=[0, 105], color_discrete_sequence=[C["ok"]])),
                           use_container_width=True, key="cc_bench")
            st.markdown(badge(f"run {bs.get('run_id')}", "cyan") + " " +
                        badge(f"{bs.get('passed', '?')}/{bs.get('total_benchmarks', '?')} passed",
                              "ok") + " " +
                        badge(f"unsupported claims {bs.get('unsupported_claims_found_pct')}%", "bad"),
                        unsafe_allow_html=True)
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

    try:
        cols = st.columns(2)
        with cols[0]:
            st.markdown(f'<div class="ex-card-title">{svg("file", 16, "#22d3ee")} '
                        'Recent historical events</div>', unsafe_allow_html=True)
            for e in recent_events(6):
                st.markdown(
                    f'<div class="ex-card" style="padding:10px 14px;margin:6px 0;">'
                    f'{event_type_badge(e["event_type"])} '
                    f'<span style="font-size:.8rem;color:var(--ex-muted)">'
                    f'{str(e["event_time"])[:10]}</span><br>'
                    f'<span style="font-size:.85rem;color:var(--ex-text)">{e["event_title"][:95]}'
                    f'</span></div>', unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f'<div class="ex-card-title">{svg("search", 16, "#a78bfa")} '
                        'Recent investigations</div>', unsafe_allow_html=True)
            for i in recent_investigations(6):
                conf = _f(i.get("overall_confidence"))
                tone = "ok" if conf >= 0.75 else "warn"
                st.markdown(
                    f'<div class="ex-card" style="padding:10px 14px;margin:6px 0;">'
                    f'{badge(f"confidence {conf:.2f}", tone)} '
                    f'{badge(i.get("decision_status") or "—", "muted")}<br>'
                    f'<span style="font-size:.85rem;color:var(--ex-text)">'
                    f'{(i.get("decision_title") or "")[:95]}</span></div>', unsafe_allow_html=True)
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

    try:
        cols = st.columns(2)
        with cols[0]:
            st.markdown(f'<div class="ex-card-title">{svg("shield", 16, "#34d399")} '
                        'Data quality status (automated gates)</div>', unsafe_allow_html=True)
            qm = quality_metrics()
            if qm:
                rows = " ".join(badge(f"{m['metric_name']} {m['metric_value']}", "ok")
                                for m in qm[:6])
                rows2 = " ".join(badge(f"{m['metric_name']} {m['metric_value']}", "ok")
                                 for m in qm[6:])
                st.markdown(f"<div style='line-height:2'>{rows}<br>{rows2}</div>",
                            unsafe_allow_html=True)
            else:
                st.markdown(empty_state("shield", "No quality metrics",
                            "gold_evaluation_metrics is empty."), unsafe_allow_html=True)
        with cols[1]:
            st.markdown(f'<div class="ex-card-title">{svg("database", 16, "#22d3ee")} '
                        'Pipeline health (medallion layers)</div>', unsafe_allow_html=True)
            pc = pipeline_counts()
            p = pc[0] if pc else {}
            cs = st.columns(4)
            cs[0].markdown(kpi_tile("Bronze issues", p.get("issues"), "raw"), unsafe_allow_html=True)
            cs[1].markdown(kpi_tile("Bronze PRs", p.get("prs"), "raw"), unsafe_allow_html=True)
            cs[2].markdown(kpi_tile("Bronze releases", p.get("releases"), "raw"), unsafe_allow_html=True)
            cs[3].markdown(kpi_tile("Silver records", p.get("silver"), "typed"), unsafe_allow_html=True)
            st.markdown(badge(f"Gold {k.get('events') and int(_f(k.get('events')))} events · "
                              f"{k.get('edges') and int(_f(k.get('edges')))} graph edges · "
                              f"idempotent MERGE ingestion", "cyan"), unsafe_allow_html=True)
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

    st.markdown(f'<div class="ex-card-title" style="margin-top:14px">{svg("alert", 16, "#f6c453")} '
                'Contradiction overview — both sides always preserved</div>', unsafe_allow_html=True)
    try:
        for cr in contradictions():
            urls = [u for u in re.split(r",\s*", str(cr.get("evidence_urls", "")).strip("[]\""))
                    if u.startswith("http")]
            with st.expander(f"{cr['category']} — {cr['subject']} ({cr['records_affected']} records)"):
                st.markdown(f"- Side A: **{cr['value_a']}**\n- Side B: **{cr['value_b']}**")
                st.markdown("\n".join(f"- {_mdlink(u)}" for u in urls) or "- no evidence URLs")
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# Module 2 — Timeline Explorer
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=300, show_spinner=False)
def timeline_bounds():
    rows = run_sql(f"""SELECT MIN(event_time) AS lo, MAX(event_time) AS hi
                     FROM {T}.gold_events""")
    return rows[0] if rows else {}


@st.cache_data(ttl=300, show_spinner=False)
def event_types():
    return [r["event_type"] for r in run_sql(
        f"SELECT DISTINCT event_type FROM {T}.gold_events ORDER BY event_type")]


@st.cache_data(ttl=300, show_spinner=False)
def tech_names():
    return [r["normalized_name"] for r in run_sql(
        f"""SELECT DISTINCT normalized_name FROM {T}.gold_technology_evolution
           ORDER BY normalized_name""")]


@st.cache_data(ttl=300, show_spinner=False)
def dev_names(limit=100):
    return [r["label"] for r in run_sql(
        f"""SELECT label FROM {T}.gold_nodes WHERE node_id LIKE 'developer:%'
           ORDER BY CAST(mention_count AS INT) DESC LIMIT {limit}""")]


def timeline_query(d1, d2, types, tech, dev, search):
    clauses = ["e.event_time BETWEEN :d1 AND :d2"]
    params = [("d1", d1, "STRING"), ("d2", d2, "STRING")]
    if types:
        ph = ", ".join(f":t{i}" for i in range(len(types)))
        clauses.append(f"e.event_type IN ({ph})")
        for i, t in enumerate(types):
            params.append((f"t{i}", t, "STRING"))
    if tech:
        clauses.append("""e.source_id IN (SELECT target_node_id FROM {T}.gold_edges
                          WHERE source_node_id = :tech)""")
        params.append(("tech", f"technology:{tech}", "STRING"))
    if dev:
        clauses.append("""e.source_id IN (SELECT target_node_id FROM {T}.gold_edges
                          WHERE source_node_id = :dev)""")
        params.append(("dev", f"developer:{dev}", "STRING"))
    if search:
        clauses.append("(LOWER(e.event_title) LIKE :q OR LOWER(COALESCE(e.event_description, '')) LIKE :q)")
        params.append(("q", f"%{search.lower()}%", "STRING"))
    sql = f"""SELECT e.event_id, e.event_type, e.event_title, e.event_description,
                     e.event_time, e.source_id, e.source_url, e.extraction_method,
                     e.confidence_score
              FROM {T}.gold_events e
              WHERE {' AND '.join(clauses)}
              ORDER BY e.event_time ASC
              LIMIT 300"""
    return run_sql(sql, params)


@st.cache_data(ttl=300, show_spinner=False)
def silver_detail(source_id):
    rows = run_sql(
        f"""SELECT state, author, created_ts, closed_ts, labels, url
           FROM {T}.silver_github_records WHERE source_id = :sid LIMIT 1""",
        [("sid", source_id, "STRING")])
    return rows[0] if rows else {}


EVENT_TONE = {"bug_discovery": "bad", "bug_resolution": "ok", "version_release": "cyan",
              "feature_introduction": "violet", "deprecation": "warn",
              "technology_migration": "violet", "security_incident": "bad",
              "performance_improvement": "cyan", "architecture_modification": "violet",
              "design_discussion": "muted", "tech_mention": "muted"}


def page_temporal():
    page_header("clock", "Temporal Intelligence",
                "Chronological historical reconstruction from gold_events — every timestamp is a "
                "real source-record time. Documented provenance is attached to each event; "
                "zoom and pan the timeline freely.")

    b = timeline_bounds()
    lo = b.get("lo") or "2025-01-01"
    hi = b.get("hi") or "2026-12-31"

    with st.form("tl_form"):
        c = st.columns(4)
        dr = c[0].date_input("Event time range",
                             (pd.Timestamp(lo).date(), pd.Timestamp(hi).date()))
        types = c[1].multiselect("Event types", event_types())
        tech = c[2].selectbox("Technology", [""] + tech_names())
        dev = c[3].selectbox("Developer", [""] + dev_names())
        search = st.text_input(
            "Search titles & descriptions",
            value=st.session_state.get("global_search", ""),
            placeholder="e.g. envvar, pydantic, vendoring")
        submitted = st.form_submit_button("Apply filters", type="primary",
                                          use_container_width=True)

    if submitted or True:  # first render uses default bounds too
        if isinstance(dr, tuple) and len(dr) == 2:
            d1, d2 = dr
            rows = timeline_query(f"{d1} 00:00:00", f"{d2} 23:59:59",
                                  types, tech, dev, search.strip())
        else:
            st.info("Please select a valid date range.")
            rows = []

        if not rows:
            st.markdown(empty_state("clock", "No events match these filters",
                        "Widen the date range or clear filters — ECHO-X never fabricates "
                        "events to fill a gap."), unsafe_allow_html=True)
            return

        st.markdown(badge(f"{len(rows)} events", "cyan") + " " +
                    badge("capped at 300", "muted") + " " +
                    badge("chronological", "ok"), unsafe_allow_html=True)

        d = pd.DataFrame(rows)
        d["event_time"] = pd.to_datetime(d["event_time"])
        d["tone"] = d["event_type"].map(EVENT_TONE).fillna("muted")
        order = list(d["event_type"].value_counts().index)

        fig = go.Figure()
        for t in order:
            sub = d[d["event_type"] == t]
            tone = EVENT_TONE.get(t, "muted")
            fig.add_trace(go.Scatter(
                x=sub["event_time"], y=[t] * len(sub), mode="markers",
                name=t.replace("_", " "),
                marker=dict(size=9, color=C[tone], line=dict(width=1, color="#060a13")),
                customdata=sub["event_title"],
                hovertemplate="<b>%{customdata}</b><br>%{y} · %{x|%Y-%m-%d}<extra></extra>"))
        fig.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)",
                          plot_bgcolor="rgba(0,0,0,0)", height=430, showlegend=True,
                          legend=dict(orientation="h", y=-0.18, font=dict(size=10)),
                          margin=dict(l=10, r=10, t=10, b=10),
                          font=dict(color="#e8eef7"),
                          xaxis=dict(gridcolor="rgba(148,163,184,.12)", zeroline=False),
                          yaxis=dict(categoryorder="array", categoryarray=order[::-1],
                                     gridcolor="rgba(148,163,184,.12)"))
        st.plotly_chart(fig, use_container_width=True, key="tl_lanes")

        st.markdown(f'<div class="ex-card-title" style="margin-top:8px">{svg("file", 16, "#22d3ee")} '
                    'Event detail — with documented provenance</div>', unsafe_allow_html=True)
        for idx, r in enumerate(rows):
            title = r.get("event_title") or r.get("event_id")
            tb = event_type_badge(r.get("event_type"))
            with st.expander(f"{str(r['event_time'])[:16]} — {title[:110]}"):
                st.markdown(tb + " " +
                            badge(f"confidence {r.get('confidence_score')}", "muted") + " " +
                            badge(f"extraction {r.get('extraction_method')}", "muted"),
                            unsafe_allow_html=True)
                if r.get("event_description"):
                    st.markdown(r["event_description"])
                st.markdown(f"Source record: {_mdlink(r.get('source_url'))}")
                if 0 < idx < len(rows) - 1:
                    st.caption(f"Previous: {rows[idx-1].get('event_title', '')[:80]} · "
                                f"Next: {rows[idx+1].get('event_title', '')[:80]}")
                sd = silver_detail(r.get("source_id"))
                if sd:
                    st.markdown(f"Underlying GitHub record — state **{sd.get('state')}**, "
                                f"author **{sd.get('author')}**, "
                                f"created {str(sd.get('created_ts'))[:16]}, "
                                f"closed {str(sd.get('closed_ts'))[:16] if sd.get('closed_ts') else '—'}")


# --------------------------------------------------------------------------- #
# Module 3 — Knowledge Graph Explorer
# --------------------------------------------------------------------------- #
@st.cache_data(ttl=300, show_spinner=False)
def hub_nodes(limit=50):
    return run_sql(f"""SELECT node_id, label, node_type, node_subtype, mention_count
                     FROM {T}.gold_nodes
                     ORDER BY CAST(mention_count AS INT) DESC NULLS LAST LIMIT {limit}""")


@st.cache_data(ttl=300, show_spinner=False)
def node_edges(node_id, limit=150):
    return run_sql(f"""SELECT edge_id, source_node_id, target_node_id,
                            relationship_type, relationship_basis, confidence_score,
                            evidence_source_id, evidence_source_url
                     FROM {T}.gold_edges
                     WHERE source_node_id = :nid OR target_node_id = :nid
                     LIMIT {limit}""",
                   [("nid", node_id, "STRING")])


@st.cache_data(ttl=300, show_spinner=False)
def nodes_by_ids(ids):
    if not ids:
        return []
    ph = ", ".join(f":i{k}" for k in range(len(ids)))
    params = [(f"i{k}", v, "STRING") for k, v in enumerate(ids)]
    return run_sql(f"""SELECT node_id, label, node_type, node_subtype, event_time,
                              mention_count, source_url FROM {T}.gold_nodes
                        WHERE node_id IN ({ph})""", params)


def _node_color(n):
    t = (n.get("node_subtype") or "") + " " + (n.get("node_id") or "")
    if t.startswith("developer") or ":developer:" in t:
        return C["dev"]
    if t.startswith("technology") or ":technology:" in t:
        return C["tech"]
    if "release" in t:
        return C["release"]
    if t.startswith("event") or ":bug_" in t or ":version_release" in t or ":feature" in t:
        return C["event"]
    return C["record"]


@st.cache_data(ttl=300, show_spinner=False)
def incident_chains():
    return run_sql(f"""SELECT bug_title, discovered_at, resolved_at,
                            resolution_days, source_url
                     FROM {T}.gold_incident_resolutions
                     WHERE resolved_at IS NOT NULL
                     ORDER BY resolved_at DESC""")


@st.cache_data(ttl=300, show_spinner=False)
def events_between(t0, t1):
    return run_sql(f"""SELECT event_type, event_title, event_time, source_url
                     FROM {T}.gold_events
                     WHERE event_time BETWEEN :a AND :b
                     ORDER BY event_time""",
                   [("a", str(t0), "STRING"), ("b", str(t1), "STRING")])


def page_graph():
    page_header("share", "Knowledge Graph Explorer",
                "Interactive graph over gold_nodes and gold_edges — draggable nodes, zoom, "
                "pan, and connected-node highlighting.")
    st.markdown(badge("documented relationships", "ok") + " " +
                badge("inferred relationships — always labeled", "warn") + " " +
                badge("only edges that exist in gold_edges", "cyan"), unsafe_allow_html=True)

    try:
        hubs = hub_nodes()
        hub_labels = {h["node_id"]: f"{h['label']} ({h['mention_count']} mentions)" for h in hubs}
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)
        return

    a, b = st.columns(2)
    sel_id = a.selectbox("Start from a hub node", list(hub_labels.keys()),
                         format_func=lambda k: hub_labels.get(k, k))
    basis = b.selectbox("Edge basis", ["all", "documented", "inferred"])
    n_edges = st.slider("Max edges shown", 30, 150, 80, 10)

    try:
        edges = [e for e in node_edges(sel_id, n_edges)]
        if basis != "all":
            edges = [e for e in edges if e.get("relationship_basis") == basis]

        node_ids = {sel_id}
        for e in edges:
            node_ids.add(e["source_node_id"])
            node_ids.add(e["target_node_id"])
        ndata = {n["node_id"]: n for n in nodes_by_ids(list(node_ids))}
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)
        return

    nodes, eobjs = [], []
    for nid, n in ndata.items():
        label = (n.get("label") or nid)[:28]
        nodes.append(ANode(id=nid, label=label,
                           size=18 + min(_i(n.get("mention_count")), 40),
                           color=_node_color(n),
                           title=f"{nid}\n{n.get('node_type')} / {n.get('node_subtype')}"))
    for e in edges:
        doc = e.get("relationship_basis") == "documented"
        eobjs.append(AEdge(source=e["source_node_id"], target=e["target_node_id"],
                           color=C["ok"] if doc else C["warn"],
                           label=e.get("relationship_type", ""),
                           width=2 if doc else 1,
                           title=(f"{e.get('relationship_type')} ({e.get('relationship_basis')}, "
                                  f"confidence {e.get('confidence_score')})\n"
                                  f"{e.get('evidence_source_url') or ''}")))
    selected = None
    if nodes:
        selected = agraph(nodes=nodes, edges=eobjs,
                          config=AGraphConfig(directed=True, physics=True,
                                              nodeHighlightBehavior=True,
                                              highlightColor="#22d3ee", height=520,
                                              collapsible=False, nodeLabel="label"))
    st.caption(f"Showing {len(edges)} relationships touching the selected node. "
                "Only relationships that exist in gold_edges are ever drawn — nothing is invented.")

    if selected and selected != sel_id:
        n = ndata.get(selected, {})
        st.markdown(f'<div class="ex-card-title">{svg("share", 16, "#a78bfa")} '
                    f'Selected node — {(n.get("label") or selected)[:60]}</div>',
                    unsafe_allow_html=True)
        st.markdown(
            "<div style='line-height:1.9'>" +
            badge(f"{n.get('node_type')} / {n.get('node_subtype') or '—'}", "violet") + " " +
            badge(f"mentions {n.get('mention_count')}", "muted") + " " +
            badge(f"event time {str(n.get('event_time'))[:10]}", "cyan") +
            "</div>", unsafe_allow_html=True)
        if n.get("source_url"):
            st.markdown(f"Source: {_mdlink(n.get('source_url'))}")
        rel = [e for e in edges if e["source_node_id"] == selected or e["target_node_id"] == selected]
        if rel:
            st.markdown('<div class="ex-kicker">Relationship evidence</div>',
                        unsafe_allow_html=True)
            st.dataframe(pd.DataFrame([{
                "relationship": e.get("relationship_type"),
                "basis": e.get("relationship_basis"),
                "confidence": e.get("confidence_score"),
                "other end": e["target_node_id"] if e["source_node_id"] == selected else e["source_node_id"],
                "evidence": e.get("evidence_source_url")} for e in rel]), use_container_width=True)

    st.markdown(f'<div class="ex-card-title" style="margin-top:12px">{svg("git", 16, "#34d399")} '
                'Bug → fix chains (incident resolution history)</div>', unsafe_allow_html=True)
    try:
        chains = incident_chains()
        if chains:
            labels = [f"{c['bug_title'][:70]} ({c['resolution_days']} days)" for c in chains]
            ci = st.selectbox("Chain", range(len(chains)),
                             format_func=lambda i: labels[i], key="chain_sel")
            c = chains[ci]
            st.markdown(
                "<div class='ex-card' style='padding:12px 16px'>" +
                badge("bug", "bad") + " → " + badge("fix", "warn") + " &nbsp; <b>" +
                (c["bug_title"] or "")[:90] + "</b> was open " +
                str(c["resolution_days"]) + " days (" +
                str(c["discovered_at"])[:10] + " → " + str(c["resolved_at"])[:10] +
                ")</div>",
                unsafe_allow_html=True)
            st.markdown(f"- Incident evidence: {_mdlink(c['source_url'])}")
            ce = events_between(c["discovered_at"], c["resolved_at"])
            if ce:
                st.caption(f"{len(ce)} documented events occurred while this bug was open:")
                st.dataframe(pd.DataFrame([{"event": e["event_title"],
                                            "type": e["event_type"],
                                            "time": str(e["event_time"])[:16],
                                            "evidence": e["source_url"]} for e in ce]),
                             use_container_width=True, key="chain_ev")
            else:
                st.caption("No documented events occurred inside this open window.")
        else:
            st.markdown(empty_state("git", "No resolved incidents",
                        "No resolved incidents found in gold_incident_resolutions."),
                        unsafe_allow_html=True)
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# Module 4 — Investigation Assistant (Genie first, deterministic ELTR fallback)
# --------------------------------------------------------------------------- #
import json


@st.cache_data(ttl=300, show_spinner=False)
def all_investigations():
    return run_sql(f"""SELECT investigation_id, decision_id, decision_title,
                            decision_date, decision_status,
                            triggering_problem_title, aligned_event_count,
                            timeline_slice, verified_facts, inferred_facts,
                            contradiction_categories, contradiction_subjects,
                            evidence_urls, overall_confidence, reconstruction_class,
                            missing_information
                     FROM {T}.gold_investigations""")


def _jlist(v):
    """Parse a JSON array string coming back from the statement API."""
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    try:
        x = json.loads(v)
        return [str(i) for i in x] if isinstance(x, list) else ([str(x)] if x else [])
    except (TypeError, ValueError):
        s = str(v).strip("[]")
        return [p.strip(' \"') for p in s.split(",") if p.strip(' \"')]


def ask_genie(question, timeout_s=150):
    """Ask the ECHO-X Genie space. Returns dict or raises."""
    w = ws_client()
    conv = w.genie.start_conversation(space_id=GENIE_SPACE_ID, content=question)
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        resp = w.genie.list_conversation_messages(
            space_id=GENIE_SPACE_ID, conversation_id=conv.conversation_id)
        msgs = getattr(resp, "messages", None) or []
        for m in msgs:
            answer = None
            evidence_sql = None
            suggested = []
            for att in (getattr(m, "attachments", None) or []):
                t = getattr(att, "text", None)
                if t is not None and getattr(t, "content", None):
                    answer = t.content
                qa = getattr(att, "query", None)
                if qa is not None and getattr(qa, "query", None):
                    evidence_sql = qa.query
                sq = getattr(att, "suggested_questions", None)
                if sq is not None and getattr(sq, "questions", None):
                    suggested = list(sq.questions)
            if answer:
                return {"answer": answer, "evidence_sql": evidence_sql,
                        "suggested": suggested,
                        "conversation_id": conv.conversation_id}
        time.sleep(5)
    raise TimeoutError("Genie did not answer within the timeout window.")


def render_investigation(inv):
    """Render a gold_investigations row in full ELTR structure."""
    conf = _f(inv.get("overall_confidence"))
    st.markdown(f'<div class="ex-card-title">{svg("search", 16, "#a78bfa")} '
                f'Investigation — {(inv.get("decision_title") or "")[:80]}</div>',
                unsafe_allow_html=True)
    st.markdown(f"`{inv.get('investigation_id')}`")
    tone = "ok" if conf >= 0.75 else ("warn" if conf >= 0.5 else "bad")
    st.markdown(badge(f"confidence {conf:.2f}", tone) + " " +
                badge(f"class {inv.get('reconstruction_class') or '—'}", "violet") + " " +
                badge(f"{inv.get('aligned_event_count')} aligned events", "cyan") + " " +
                badge(f"status {inv.get('decision_status') or '—'}", "muted"),
                unsafe_allow_html=True)
    st.caption(f"Decision date {str(inv.get('decision_date'))[:10]} · timeline slice "
               f"{inv.get('timeline_slice')}")

    vf = _jlist(inv.get("verified_facts"))
    inf = _jlist(inv.get("inferred_facts"))
    ev = [u for u in _jlist(inv.get("evidence_urls")) if str(u).startswith("http")]
    gaps = _jlist(inv.get("missing_information"))
    cc = _jlist(inv.get("contradiction_categories"))
    cs = _jlist(inv.get("contradiction_subjects"))

    a, b = st.columns(2)
    with a:
        st.markdown(f'<div class="ex-kicker">{svg("check", 13, "#34d399")} '
                    'Verified facts — documented evidence</div>', unsafe_allow_html=True)
        st.markdown("\n".join(f"- {x}" for x in vf) or "- none")
    with b:
        st.markdown(f'<div class="ex-kicker">{svg("sparkle", 13, "#a78bfa")} '
                    'Inferred facts — clearly labeled, confidence-scored</div>', unsafe_allow_html=True)
        st.markdown("\n".join(f"- {x}" for x in inf) or "- none")
    st.markdown(f'<div class="ex-kicker">{svg("external", 13, "#22d3ee")} Evidence</div>',
                unsafe_allow_html=True)
    st.markdown("\n".join(f"- {_mdlink(u)}" for u in ev) or "- no evidence URLs")
    if cc or cs:
        st.markdown(f'<div class="ex-kicker">{svg("alert", 13, "#f6c453")} Contradictions</div>',
                    unsafe_allow_html=True)
        st.markdown(f"Categories: {', '.join(cc) if cc else '—'} · Subjects: {', '.join(cs) if cs else '—'}")
    st.markdown(f'<div class="ex-kicker">{svg("info", 13, "#93a1b8")} '
                'Declared gaps — what could NOT be reconstructed</div>', unsafe_allow_html=True)
    st.markdown("\n".join(f"- {x}" for x in gaps) or "- no gaps recorded")


def investigate_fallback(q):
    """Deterministic, evidence-only answer routing over the Gold layer."""
    ql = q.lower()
    st.markdown("#### Deterministic ELTR answer (no generative model used)")

    if "vendor" in ql or "click" in ql:
        rows = [i for i in all_investigations()
                if "vendor" in (i.get("decision_title") or "").lower()]
        if rows:
            render_investigation(rows[0])
            return

    if "pydantic" in ql:
        ev = run_sql(f"""SELECT normalized_name, year, mentions
                       FROM {T}.gold_technology_evolution
                       WHERE normalized_name = :n ORDER BY year""",
                     [("n", "pydantic", "STRING")])
        if ev:
            st.markdown(f"Pydantic was mentioned in {len(ev)} years of project history. "
                        f"Peak mentions: {max(_f(e['mentions']) for e in ev)} "
                        f"in {max(ev, key=lambda e: _f(e['mentions']))['year']}.")
            st.dataframe(pd.DataFrame(ev), use_container_width=True)
            st.caption("Every row comes from gold_technology_evolution with per-mention provenance "
                       "in silver_github_records — see the Timeline Explorer for the source URLs.")
            return

    ver = re.search(r"\d+\.\d+(\.\d+)?", q)
    if ver and ("before" in ql or "after" in ql or "around" in ql or "release" in ql):
        v = ver.group(0)
        rel = run_sql(f"""SELECT event_title, event_time, event_description, source_url
                       FROM {T}.gold_events
                       WHERE event_type = 'version_release' AND event_title LIKE :v
                       ORDER BY event_time DESC LIMIT 1""",
                     [("v", f"%{v}%", "STRING")])
        if rel:
            r0 = rel[0]
            rt = pd.Timestamp(r0["event_time"])
            around = run_sql(f"""SELECT event_type, event_title, event_time, source_url
                            FROM {T}.gold_events
                            WHERE event_time BETWEEN :a AND :b
                            ORDER BY event_time""",
                            [("a", str(rt - pd.Timedelta(days=30)), "STRING"),
                             ("b", str(rt + pd.Timedelta(days=30)), "STRING")])
            st.markdown(f"Release **{r0['event_title']}** shipped {str(r0['event_time'])[:10]} "
                        f"({_mdlink(r0['source_url'])}). Within ±30 days, "
                        f"{len(around)} historical events are recorded:")
            st.dataframe(pd.DataFrame(around), use_container_width=True)
            return

    if "contradict" in ql or "conflict" in ql:
        cons = run_sql(f"""SELECT category, subject, attribute, value_a, value_b,
                                records_affected, evidence_urls
                         FROM {T}.contradiction_records""")
        if cons:
            st.markdown("ECHO-X found the following contradictions and preserved **both sides** "
                        "instead of picking one:")
            for c in cons:
                st.markdown(f"- **{c['subject']}** ({c['category']}): `{c['value_a']}` vs "
                            f"`{c['value_b']}` — {c['records_affected']} records affected")
                for u in [x for x in str(c.get("evidence_urls", "")).split(",")
                          if x.strip().startswith("http")]:
                    st.markdown(f"  - {_mdlink(u.strip())}")
            return

    if "unresolved" in ql or "open" in ql or "decision" in ql:
        unc = [i for i in all_investigations()
               if (i.get("decision_status") or "").lower() == "unresolved"]
        if unc:
            st.markdown(f"{len(unc)} decision(s) remain unresolved. ECHO-X keeps them "
                        "visible with their declared gaps rather than guessing:")
            for i in unc:
                render_investigation(i)
            return

    if "bug" in ql or "trace" in ql:
        kw = [w for w in re.findall(r"[a-z_]{4,}", ql)
              if w not in ("what", "when", "which", "where", "trace", "this", "that",
                           "with", "from", "about", "please")][:2]
        if kw:
            ph = " AND ".join(f"LOWER(bug_title) LIKE :k{i}" for i in range(len(kw)))
            params = [(f"k{i}", f"%{k}%", "STRING") for i, k in enumerate(kw)]
            chains = run_sql(f"""SELECT bug_title, discovered_at, resolved_at,
                                        resolution_days, source_url
                                 FROM {T}.gold_incident_resolutions
                                 WHERE resolved_at IS NOT NULL AND {ph}
                                 LIMIT 5""", params)
            if chains:
                for ch in chains:
                    st.markdown(badge("bug", "bad") + " → " + badge("fix", "warn"),
                                unsafe_allow_html=True)
                    st.markdown(f"**{(ch['bug_title'] or '')[:90]}** — open "
                                f"{ch['resolution_days']} days "
                                f"({str(ch['discovered_at'])[:10]} → "
                                f"{str(ch['resolved_at'])[:10]}). "
                                f"Evidence: {_mdlink(ch['source_url'])}")
                return

    words = [w for w in re.findall(r"[a-z0-9_.]{3,}", ql)][:3]
    if words:
        ph = " OR ".join([f"LOWER(event_title) LIKE :w{i}" for i in range(len(words))])
        params = [(f"w{i}", f"%{w}%", "STRING") for i, w in enumerate(words)]
        hits = run_sql(f"""SELECT event_title AS title, event_time AS ts, source_url AS url
                        FROM {T}.gold_events WHERE {ph}
                        LIMIT 10""", params)
        if hits:
            st.markdown("These documented events best match your question:")
            st.dataframe(pd.DataFrame(hits), use_container_width=True)
            st.caption("Refine the question or use the Timeline Explorer to search the full history.")
            return

    st.warning("Insufficient evidence in the reconstructed history to answer this question. "
               "ECHO-X will not fabricate an answer — try asking about a specific technology, "
               "release, bug, decision, or contradiction.")


STARTERS = ["Why was Click vendoring introduced?",
            "What happened before release 0.26.0?",
            "How did Pydantic evolve?",
            "Which decisions remain unresolved?",
            "What contradictions exist in the historical records?"]

STOPWORDS = {"what", "when", "which", "where", "does", "with", "from", "about",
             "this", "that", "were", "there", "their", "have", "been", "will",
             "would", "could", "should", "why", "how", "was", "the", "and", "for",
             "exist", "historical", "records", "release", "before", "happened"}


def page_studio():
    page_header("search", "Investigation Studio",
                "Ask questions about the reconstructed history — Genie provides the narrative; "
                "if it is unavailable, a deterministic evidence lookup answers instead. "
                "Unsupported claims are never generated.")

    if "question" not in st.session_state:
        st.session_state.question = STARTERS[0]
    st.markdown('<div class="ex-kicker">Suggested investigations</div>', unsafe_allow_html=True)
    chips = st.columns(len(STARTERS))
    for i, s in enumerate(STARTERS):
        if chips[i].button(s, use_container_width=True):
            st.session_state.question = s
            st.rerun()

    q = st.text_area("Your question", value=st.session_state.question, height=80)
    mode = st.radio("Answer engine", ["Genie (narrative)", "Deterministic ELTR"], horizontal=True)
    ask = st.button("Investigate", type="primary", use_container_width=True)

    if ask and q.strip():
        center, right = st.columns([3, 1.15])
        with center:
            if mode.startswith("Genie"):
                try:
                    with st.spinner("Asking the Genie investigation assistant… (may take ~1 minute)"):
                        res = ask_genie(q.strip())
                    st.markdown(badge("Genie Assistant", "ok") + " " +
                                badge("evidence-linked", "cyan"), unsafe_allow_html=True)
                    st.markdown(res["answer"])
                    if res.get("evidence_sql"):
                        with st.expander("Evidence SQL generated by Genie"):
                            st.code(res["evidence_sql"], language="sql")
                    if res.get("suggested"):
                        st.markdown('<div class="ex-kicker">Suggested follow-ups</div>',
                                    unsafe_allow_html=True)
                        st.markdown(" · ".join(res["suggested"]))
                    st.caption(f"conversation `{res['conversation_id']}`")
                except Exception as e:
                    st.warning(f"Genie is not accessible right now ({type(e).__name__}). "
                               "Falling back to the deterministic ELTR engine — "
                               "your answer will still be evidence-only.")
                    investigate_fallback(q)
            else:
                investigate_fallback(q)
        with right:
            st.markdown('<div class="ex-kicker">Related evidence — Gold tables</div>',
                        unsafe_allow_html=True)
            words = list(dict.fromkeys(
                w for w in re.findall(r"[a-z0-9_.]{4,}", q.lower())
                if w not in STOPWORDS))[:4]
            if words:
                ph = " OR ".join(f"LOWER(event_title) LIKE :q{i}" for i in range(len(words)))
                params = [(f"q{i}", f"%{w}%", "STRING") for i, w in enumerate(words)]
                try:
                    hits = run_sql(f"""SELECT event_title, event_type, event_time, source_url
                                    FROM {T}.gold_events WHERE {ph}
                                    ORDER BY event_time DESC LIMIT 8""", params)
                except Exception:
                    hits = []
                if hits:
                    for h in hits:
                        st.markdown(
                            f'<div class="ex-card" style="padding:9px 12px;margin:6px 0;">'
                            f'{event_type_badge(h["event_type"])} '
                            f'<span style="font-size:.7rem;color:var(--ex-faint)">'
                            f'{str(h["event_time"])[:10]}</span><br>'
                            f'<span style="font-size:.78rem;color:var(--ex-text)">'
                            f'{(h["event_title"] or "")[:90]}</span></div>',
                            unsafe_allow_html=True)
                else:
                    st.markdown(empty_state("search", "No direct keyword matches",
                                "No gold_events titles matched the question keywords — "
                                "the answer engine may still find structured matches."),
                                unsafe_allow_html=True)
            else:
                st.markdown(empty_state("search", "No keywords detected",
                            "Ask about a technology, version, developer, bug, or decision."),
                            unsafe_allow_html=True)
    elif ask:
        st.info("Type a question first — try one of the starter questions above.")


# --------------------------------------------------------------------------- #
# Module 5 — Evaluation & Quality
# --------------------------------------------------------------------------- #
def page_evaluation():
    page_header("gauge", "Evaluation & Quality",
                "How trustworthy is the reconstruction? Every metric comes from "
                "gold_evaluation_metrics, gold_benchmark_summary, and gold_benchmark_results — "
                "no numbers are invented in this app.")

    try:
        hist = benchmark_history()
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)
        return
    if not hist:
        st.markdown(empty_state("gauge", "No benchmark runs recorded",
                    "gold_benchmark_summary is empty — run the evaluation pipeline first."),
                    unsafe_allow_html=True)
        return

    runs = [h["run_id"] for h in hist]
    sel = st.selectbox("Benchmark run", runs, index=0)
    h = next((x for x in hist if x["run_id"] == sel), {})

    cs = st.columns(5)
    cs[0].markdown(kpi_tile("Benchmarks", h.get("total_benchmarks")), unsafe_allow_html=True)
    cs[1].markdown(kpi_tile("Passed", h.get("passed")), unsafe_allow_html=True)
    cs[2].markdown(kpi_tile("Failed", h.get("failed")), unsafe_allow_html=True)
    cs[3].markdown(kpi_tile("Accuracy", f"{_f(h.get('accuracy_pct')):.0f}%"), unsafe_allow_html=True)
    cs[4].markdown(kpi_tile("Unsupported claims", f"{_f(h.get('unsupported_claims_found_pct')):.0f}%",
                            "lower is better"), unsafe_allow_html=True)

    try:
        results = benchmark_results_for(sel)
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)
        results = []

    df = pd.DataFrame(results)
    if not df.empty:
        dims = [("Answer correctness", "answer_correctness"),
                ("Evidence coverage", "evidence_coverage"),
                ("Source traceability", "source_traceability"),
                ("Temporal accuracy", "temporal_accuracy"),
                ("Relationship accuracy", "relationship_accuracy"),
                ("Contradiction handling", "contradiction_handling")]
        rows = []
        for label, col in dims:
            if col in df.columns:
                v = pd.to_numeric(df[col], errors="coerce")
                if v.notna().any():
                    rows.append(("dimension", label, v.mean()))
        bd = pd.DataFrame(rows, columns=["k", "dimension", "pct"])
        if not bd.empty:
            st.markdown(f'<div class="ex-card-title">{svg("gauge", 16, "#34d399")} '
                        f'Quality dimensions — run {sel} (mean over {len(df)} benchmark results)'
                        '</div>', unsafe_allow_html=True)
            st.plotly_chart(_fig(px.bar(bd, x="pct", y="dimension", orientation="h",
                           range_x=[0, 105], color_discrete_sequence=[C["ok"]])),
                           use_container_width=True, key="ev_dims")

        st.markdown(f'<div class="ex-card-title">{svg("file", 16, "#22d3ee")} '
                    'Per-question results</div>', unsafe_allow_html=True)
        for r in results:
            ok = str(r.get("passed", "")).lower() in ("true", "1", "yes", "t")
            hdr = (f"{r.get('question_id')} · {(r.get('category') or '—')} — "
                   f"score {r.get('score')} ({r.get('checks_passed')}/{r.get('checks_total')} checks)")
            with st.expander(hdr):
                st.markdown(badge("passed" if ok else "failed", "ok" if ok else "bad") + " " +
                            badge(f"unsupported claim rate {r.get('unsupported_claim_rate')}",
                                  "warn" if _f(r.get("unsupported_claim_rate")) > 0 else "ok"),
                            unsafe_allow_html=True)
                if r.get("answer_summary"):
                    st.markdown(r["answer_summary"])
                if r.get("failure_explanation"):
                    st.markdown(f"**Failure explanation:** {r['failure_explanation']}")

    st.markdown(f'<div class="ex-card-title" style="margin-top:12px">{svg("shield", 16, "#34d399")} '
                'Data quality gates (gold_evaluation_metrics)</div>', unsafe_allow_html=True)
    try:
        qm = quality_metrics()
        if qm:
            for m in qm:
                with st.expander(f"{m['metric_name']} — {m['metric_value']}"):
                    st.markdown(m.get("metric_meaning") or "—")
        else:
            st.markdown(empty_state("shield", "No quality metrics",
                        "gold_evaluation_metrics is empty."), unsafe_allow_html=True)
    except Exception as e:
        st.markdown(error_state(e), unsafe_allow_html=True)

    if len(hist) > 1:
        st.markdown(f'<div class="ex-card-title">{svg("git", 16, "#a78bfa")} '
                    'Run history</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame(hist), use_container_width=True, key="ev_hist")


# --------------------------------------------------------------------------- #
# About — the ELTR methodology
# --------------------------------------------------------------------------- #
def page_about():
    page_header("info", "About ECHO-X & the ELTR Methodology",
                "Evidence-Linked Temporal Reconstruction — how fragmented records become "
                "a trustworthy historical narrative.")
    st.markdown("""**ECHO-X** reconstructs forgotten project history from fragmented records
using **ELTR — Evidence-Linked Temporal Reconstruction**:

1. **Bronze ingestion** — raw GitHub issues, PRs, and releases land in `bronze_github_*` tables.
2. **Silver cleaning** — deduplication, type normalization, and union into `silver_github_records`.
3. **Event & entity extraction** — bugs, features, releases, technologies, and developers become
   structured events (`gold_events`) and entities (`gold_entities`).
4. **Temporal intelligence** — timelines and technology evolution (`gold_timeline`,
   `gold_technology_evolution`) are anchored on **real timestamps only**; relative claims are
   never converted into fabricated dates.
5. **Knowledge graph** — `gold_nodes` and `gold_edges` link people, technologies, bugs, fixes,
   and releases. Every edge is labeled **documented** (direct evidence) or **inferred**
   (pattern-based), each with a confidence score.
6. **Decision reconstruction** — `gold_decisions` recovers the reasoning behind releases.
7. **Contradiction detection** — `contradiction_records` preserves **both sides** of every
   conflicting claim, with the affected records and evidence URLs.
8. **Evidence-linked investigations** — `gold_investigations` assembles each reconstruction:
   verified facts, inferred facts, evidence URLs, an overall confidence score, and an explicit
   list of **what could not be reconstructed** (declared gaps).
9. **Automated benchmarking** — `gold_benchmark_questions`, `gold_benchmark_results`, and
   `gold_benchmark_summary` score correctness, evidence coverage, traceability, temporal
   accuracy, relationship accuracy, contradiction handling, and unsupported claims.

**This app is strictly read-only** over the Gold layer: it never writes, overwrites, or deletes
historical data. Where evidence is missing, ECHO-X says so instead of guessing.""")
    st.markdown('<div class="ex-kicker">Pipeline</div>', unsafe_allow_html=True)
    st.markdown(badge("bronze · raw", "muted") + " → " +
                badge("silver · typed", "cyan") + " → " +
                badge("gold · events / entities / graph", "ok") + " → " +
                badge("evaluation · benchmarks", "violet"), unsafe_allow_html=True)
    st.caption(f"Data: `{CATALOG}.{SCHEMA}` · SQL warehouse `{WAREHOUSE_ID}` · "
               f"Genie space `{GENIE_SPACE_ID}`")


# --------------------------------------------------------------------------- #
# App router — premium navigation with Material icons
# --------------------------------------------------------------------------- #
def main():
    with st.sidebar:
        st.markdown(f'<div class="ex-brand"><div class="ex-brand-mark">{LOGO_SVG}</div>'
                    '<div><div class="ex-brand-name">ECHO-X</div>'
                    '<div class="ex-brand-sub">Temporal Intelligence</div></div></div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="ex-side-divider"></div>', unsafe_allow_html=True)
    pg = st.navigation([
        st.Page(page_command, title="Command Center", icon=":material/space_dashboard:",
                default=True),
        st.Page(page_temporal, title="Temporal Intelligence", icon=":material/schedule:"),
        st.Page(page_graph, title="Knowledge Graph", icon=":material/hub:"),
        st.Page(page_studio, title="Investigation Studio", icon=":material/travel_explore:"),
        st.Page(page_evaluation, title="Evaluation & Quality", icon=":material/analytics:"),
        st.Page(page_about, title="About & Methodology", icon=":material/info:"),
    ])
    with st.sidebar:
        st.markdown('<div class="ex-side-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="ex-kicker" style="margin-left:4px">Connection</div>',
                    unsafe_allow_html=True)
        st.caption(f"catalog `{CATALOG}`\nschema `{SCHEMA}`")
        st.caption("🔒 read-only over Gold")
        st.markdown('<div class="ex-kicker" style="margin-left:4px;margin-top:8px">Engine</div>',
                    unsafe_allow_html=True)
        st.caption("Evidence-Linked Temporal Reconstruction (ELTR)")
    pg.run()


main()