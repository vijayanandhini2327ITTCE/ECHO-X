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

st.set_page_config(page_title="ECHO-X", page_icon="🛰️", layout="wide")

CATALOG = os.environ.get("ECHO_X_CATALOG", "learn_get_started_de_vijayanandhini")
SCHEMA = os.environ.get("ECHO_X_SCHEMA", "echo_x")
WAREHOUSE_ID = os.environ.get("ECHO_X_WAREHOUSE_ID", "db585e0c39b6753f")
GENIE_SPACE_ID = os.environ.get("ECHO_X_GENIE_SPACE_ID",
                                "01f1d53cbc42176ca9180944796f7cfd")
T = f"{CATALOG}.{SCHEMA}"  # table prefix

C = {"accent": "#58a6ff", "ok": "#3fb950", "warn": "#d29922",
     "bad": "#f85149", "muted": "#8b949e", "dev": "#bc8cff", "event": "#58a6ff",
     "tech": "#3fb950", "record": "#8b949e", "release": "#39c5cf"}

st.markdown("""<style>
  .echo-section {border-left:3px solid #58a6ff;padding-left:12px;margin:8px 0 2px;}
  div[data-testid="stExpander"] {background:#161b22;border:1px solid #21262d;}
  .stSidebar {border-right:1px solid #21262d;}
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
# Module 1 — Intelligence Dashboard
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


def page_dashboard():
    st.markdown('<h2 class="echo-section">Intelligence Dashboard</h2>',
                unsafe_allow_html=True)
    st.caption("Every metric is computed live from the ECHO-X Gold tables — nothing is hardcoded.")
    k = kpis()
    for i, (label, val, hint) in enumerate([
            ("Historical records", k.get("records"), "silver_github_records"),
            ("Extracted events", k.get("events"), "gold_events"),
            ("Entities", k.get("entities"), "gold_entities"),
            ("Graph nodes", k.get("nodes"), "gold_nodes"),
            ("Graph edges", k.get("edges"), "gold_edges"),
            ("Decisions", k.get("decisions"), "gold_decisions"),
            ("Contradictions", k.get("contradictions"), "contradiction_records"),
            ("Investigations", k.get("investigations"), "gold_investigations"),
            ("Avg confidence", k.get("avg_confidence"), "mean overall_confidence"),
            ("Benchmark accuracy", k.get("benchmark_accuracy"), "latest run, %")]):
        st.metric(label, val if val is not None else "—", help=hint)
    st.markdown("---")

    cols = st.columns(2)
    with cols[0]:
        st.markdown("#### Events by type")
        d = pd.DataFrame(events_by_type())
        if not d.empty:
            d["n"] = pd.to_numeric(d["n"], errors="coerce")
            st.plotly_chart(_fig(px.bar(d, x="n", y="event_type", orientation="h",
                           color_discrete_sequence=[C["accent"]])), use_container_width=True)
    with cols[1]:
        st.markdown("#### Technology evolution over time")
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
                               color="normalized_name", markers=True)),
                               use_container_width=True)

    st.markdown("#### Bug → fix → release statistics")
    ir = pd.DataFrame(incident_stats())
    if not ir.empty:
        ir["resolution_days"] = pd.to_numeric(ir["resolution_days"], errors="coerce")
        m = st.columns(4)
        m[0].metric("Incident chains", len(ir))
        m[1].metric("Resolved", int(ir["resolved_at"].notna().sum()))
        m[2].metric("Unresolved (kept visible)", int(ir["resolved_at"].isna().sum()))
        m[3].metric("Avg resolution days",
                    round(ir["resolution_days"].mean(), 1) if ir["resolution_days"].notna().any() else "—")
        rr = ir[ir["resolution_days"].notna()]
        if not rr.empty:
            st.plotly_chart(_fig(px.histogram(rr, x="resolution_days", nbins=20,
                           color_discrete_sequence=[C["accent"]])), use_container_width=True)

    st.markdown("#### Developer contribution statistics (documented authorship links)")
    dd = pd.DataFrame(top_developers())
    if not dd.empty:
        dd["mention_count"] = pd.to_numeric(dd["mention_count"], errors="coerce")
        st.plotly_chart(_fig(px.bar(dd, x="mention_count", y="label", orientation="h",
                       color_discrete_sequence=[C["dev"]
                       ])), use_container_width=True)

    cols = st.columns(2)
    with cols[0]:
        st.markdown("#### Decision status distribution")
        ds = pd.DataFrame(decision_status())
        if not ds.empty:
            ds["n"] = pd.to_numeric(ds["n"], errors="coerce")
            st.plotly_chart(_fig(px.pie(ds, names="decision_status", values="n", hole=0.55,
                           color_discrete_sequence=[C["ok"], C["warn"], C["bad"]])), use_container_width=True)
    with cols[1]:
        st.markdown("#### Benchmark performance (latest run)")
        bs = benchmark_summary()
        if bs:
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
                           use_container_width=True)
            st.caption(f"Run {bs.get('run_id')} — {bs.get('passed', '?')}/{bs.get('total_benchmarks', '?')} benchmarks passed; "
                       f"unsupported claims found: {bs.get('unsupported_claims_found_pct')}%")

    st.markdown("#### Contradiction overview (both sides always preserved)")
    for cr in contradictions():
        urls = [u for u in re.split(r",\s*", str(cr.get("evidence_urls", "")).strip("[]\""))
                if u.startswith("http")]
        with st.expander(f"{cr['category']} — {cr['subject']} ({cr['records_affected']} records)"):
            st.markdown(f"- Side A: **{cr['value_a']}**\n- Side B: **{cr['value_b']}**")
            st.markdown("\n".join(f"- {_mdlink(u)}" for u in urls) or "- no evidence URLs")

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


def page_timeline():
    st.markdown('<h2 class="echo-section">Timeline Explorer</h2>',
                unsafe_allow_html=True)
    st.caption("Chronological historical reconstruction from gold_events, with documented provenance per event.")

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
        search = st.text_input("Search titles & descriptions", "",
                               placeholder="e.g. envvar, pydantic, vendoring")
        submitted = st.form_submit_button("Apply filters", use_container_width=True)

    if submitted or True:  # first render uses default bounds too
        if isinstance(dr, tuple) and len(dr) == 2:
            d1, d2 = dr
            rows = timeline_query(f"{d1} 00:00:00", f"{d2} 23:59:59",
                                  types, tech, dev, search.strip())
        else:
            st.info("Please select a valid date range.")
            rows = []

        st.markdown(f"**{len(rows)} events** (chronological, capped at 300)")
        if not rows:
            st.info("No events match these filters. Widen the date range or clear filters — "
                    "ECHO-X never fabricates events to fill a gap.")
        for r in rows:
            title = r.get("event_title") or r.get("event_id")
            icon = {"bug_discovery": "🐞", "bug_resolution": "✅",
                    "version_release": "📦", "feature_introduction": "✨",
                    "tech_mention": "💬"}.get(r.get("event_type"), "•")
            with st.expander(f"{icon} {str(r['event_time'])[:16]} — {title}"):
                st.markdown(f"**Type:** {r.get('event_type')}  |  "
                             f"**Confidence:** {r.get('confidence_score')}  |  "
                             f"**Extraction:** {r.get('extraction_method')}")
                if r.get("event_description"):
                    st.markdown(r["event_description"])
                st.markdown(f"Source record: {_mdlink(r.get('source_url'))}")
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
def bug_chains():
    return run_sql(f"""SELECT bug_discovery, bug_title, release_title,
                            release_date, bug_url, release_url, resolution_days
                     FROM {T}.gold_incident_resolutions
                     WHERE release_title IS NOT NULL
                     ORDER BY release_date DESC""")


@st.cache_data(ttl=300, show_spinner=False)
def chain_events(bug_discovery, release_title):
    return run_sql(f"""SELECT e.event_id, e.event_type, e.event_title, e.event_time,
                              e.source_url, e.confidence_score
                     FROM {T}.gold_events e
                     WHERE e.event_id = :b OR e.event_title = :r
                     ORDER BY e.event_time""",
                   [("b", bug_discovery, "STRING"), ("r", release_title, "STRING")])


def page_graph():
    st.markdown('<h2 class="echo-section">Knowledge Graph Explorer</h2>',
                unsafe_allow_html=True)
    st.caption(f"Directly from gold_nodes and gold_edges. Green edges = documented evidence; "
                f"amber edges = inferred relationships (always labeled).")

    hubs = hub_nodes()
    hub_labels = {h["node_id"]: f"{h['label']} ({h['mention_count']} mentions)" for h in hubs}

    a, b = st.columns(2)
    sel_id = a.selectbox("Start from a hub node", list(hub_labels.keys()),
                         format_func=lambda k: hub_labels.get(k, k))
    basis = b.selectbox("Edge basis", ["all", "documented", "inferred"])
    n_edges = st.slider("Max edges shown", 30, 150, 80, 10)

    edges = [e for e in node_edges(sel_id, n_edges)]
    if basis != "all":
        edges = [e for e in edges if e.get("relationship_basis") == basis]

    node_ids = {sel_id}
    for e in edges:
        node_ids.add(e["source_node_id"])
        node_ids.add(e["target_node_id"])
    ndata = {n["node_id"]: n for n in nodes_by_ids(list(node_ids))}

    nodes, eobjs = [], []
    for nid, n in ndata.items():
        label = (n.get("label") or nid)[:28]
        nodes.append(ANode(id=nid, label=label, size=18 + min(_i(n.get("mention_count")) , 40),
                           color=_node_color(n),
                           title=f"{nid}\n{n.get('node_type')} / {n.get('node_subtype')}"))
    for e in edges:
        doc = e.get("relationship_basis") == "documented"
        eobjs.append(AEdge(source=e["source_node_id"], target=e["target_node_id"],
                           color=C["ok"] if doc else C["warn"],
                           label=e.get("relationship_type", ""),
                           title=(f"{e.get('relationship_type')} ({e.get('relationship_basis')}, "
                                  f"confidence {e.get('confidence_score')})\n"
                                  f"{e.get('evidence_source_url') or ''}")))
    selected = None
    if nodes:
        selected = agraph(nodes=nodes, edges=eobjs,
                          config=AGraphConfig(directed=True, physics=True,
                                             nodeHighlightBehavior=True,
                                             highlightColor="#F7A7A6", height=500,
                                             collapsible=False, nodeLabel="label"))
    st.caption(f"Showing {len(edges)} relationships touching the selected node. "
                "Only relationships that exist in gold_edges are ever drawn — nothing is invented.")

    if selected and selected != sel_id:
        st.markdown(f"#### Selected: {ndata.get(selected, {}).get('label', selected)}")
        n = ndata.get(selected, {})
        st.markdown(f"- Type: {n.get('node_type')} / {n.get('node_subtype')}"
                    f"\n- Event time: {n.get('event_time')}"
                    f"\n- Mentions: {n.get('mention_count')}"
                    f"\n- Source: {_mdlink(n.get('source_url'))}")
        rel = [e for e in edges if e["source_node_id"] == selected or e["target_node_id"] == selected]
        st.dataframe(pd.DataFrame([{
            "relationship": e.get("relationship_type"),
            "basis": e.get("relationship_basis"),
            "confidence": e.get("confidence_score"),
            "other end": e["target_node_id"] if e["source_node_id"] == selected else e["source_node_id"],
            "evidence": e.get("evidence_source_url")} for e in rel]), use_container_width=True)

    st.markdown("#### Bug → fix → release chains")
    chains = bug_chains()
    if chains:
        labels = [f"{c['bug_title'][:60]} → {c['release_title'][:20]}" for c in chains]
        ci = st.selectbox("Chain", range(len(chains)), format_func=lambda i: labels[i])
        c = chains[ci]
        st.markdown(f"🐞 **{c['bug_title']}** discovered, fixed, shipped in "
                    f"📦 **{c['release_title']}** on {str(c['release_date'])[:10]} "
                    f"(resolution: {c['resolution_days']} days)")
        st.markdown(f"- Bug evidence: {_mdlink(c['bug_url'])}\n- Release evidence: {_mdlink(c['release_url'])}")
        ce = chain_events(c["bug_discovery"], c["release_title"])
        st.dataframe(pd.DataFrame([{ "event": e["event_title"],
                                    "type": e["event_type"],
                                    "time": str(e["event_time"])[:16],
                                    "confidence": e["confidence_score"],
                                    "evidence": e["source_url"]} for e in ce]),
                     use_container_width=True)
    else:
        st.info("No completed bug → fix → release chains found in gold_incident_resolutions.")

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
    st.markdown(f"**Investigation `{inv.get('investigation_id')}` — {inv.get('decision_title')}**")
    m = st.columns(4)
    m[0].metric("Overall confidence", f"{conf:.2f}")
    m[1].metric("Reconstruction class", inv.get("reconstruction_class"))
    m[2].metric("Aligned events", inv.get("aligned_event_count"))
    m[3].metric("Decision status", inv.get("decision_status"))
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
        st.markdown("##### ✅ Verified facts (documented evidence)")
        st.markdown("\n".join(f"- {x}" for x in vf) or "- none")
    with b:
        st.markdown("##### 🧠 Inferred facts (clearly labeled, confidence-scored)")
        st.markdown("\n".join(f"- {x}" for x in inf) or "- none")
    st.markdown("##### 🔗 Evidence")
    st.markdown("\n".join(f"- {_mdlink(u)}" for u in ev) or "- no evidence URLs")
    if cc or cs:
        st.markdown("##### ⚠️ Contradictions")
        st.markdown(f"Categories: {', '.join(cc) if cc else '—'} · Subjects: {', '.join(cs) if cs else '—'}")
    st.markdown("##### 🕳️ Declared gaps (what could NOT be reconstructed)")
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
            chains = run_sql(f"""SELECT bug_title, bug_discovery, release_title,
                                        release_date, resolution_days, bug_url, release_url
                                 FROM {T}.gold_incident_resolutions
                                 WHERE release_title IS NOT NULL AND {ph}
                                 LIMIT 5""", params)
            if chains:
                for ch in chains:
                    st.markdown(f"🐞 **{ch['bug_title']}** → fixed and shipped in "
                                f"📦 **{ch['release_title']}** ({str(ch['release_date'])[:10]}, "
                                f"{ch['resolution_days']} days). "
                                f"Bug evidence: {_mdlink(ch['bug_url'])} · "
                                f"Release evidence: {_mdlink(ch['release_url'])}")
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


def page_assistant():
    st.markdown('<h2 class="echo-section">Investigation Assistant</h2>',
                unsafe_allow_html=True)
    st.caption("Answers are grounded in the ECHO-X Gold layer. Genie provides the narrative; "
               "if it is unavailable, a deterministic evidence lookup answers instead. "
               "Unsupported claims are never generated.")

    if "question" not in st.session_state:
        st.session_state.question = STARTERS[0]
    chips = st.columns(len(STARTERS))
    for i, s in enumerate(STARTERS):
        if chips[i].button(s, use_container_width=True):
            st.session_state.question = s
            st.rerun()

    q = st.text_area("Your question", value=st.session_state.question, height=80)
    mode = st.radio("Answer engine", ["Genie (narrative)", "Deterministic ELTR"], horizontal=True)
    ask = st.button("Investigate", type="primary", use_container_width=True)

    if ask and q.strip():
        if mode.startswith("Genie"):
            try:
                with st.spinner("Asking the Genie investigation assistant… (may take ~1 minute)"):
                    res = ask_genie(q.strip())
                st.success("Answered by the Genie Assistant (evidence-linked).")
                st.markdown(res["answer"])
                if res.get("evidence_sql"):
                    with st.expander("Evidence SQL generated by Genie"):
                        st.code(res["evidence_sql"], language="sql")
                if res.get("suggested"):
                    st.markdown("**Suggested follow-ups:** " + " · ".join(res["suggested"]))
                st.caption(f"conversation `{res['conversation_id']}`")
            except Exception as e:
                st.warning(f"Genie is not accessible right now ({type(e).__name__}). "
                           "Falling back to the deterministic ELTR engine — "
                           "your answer will still be evidence-only.")
                investigate_fallback(q)
        else:
            investigate_fallback(q)
    elif ask:
        st.info("Type a question first — try one of the starter questions above.")


# --------------------------------------------------------------------------- #
# About — the ELTR methodology
# --------------------------------------------------------------------------- #
def page_about():
    st.markdown('<h2 class="echo-section">About ECHO-X & the ELTR Methodology</h2>',
                unsafe_allow_html=True)
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
    st.caption(f"Data: `{CATALOG}.{SCHEMA}` · SQL warehouse `{WAREHOUSE_ID}` · "
               f"Genie space `{GENIE_SPACE_ID}`")


# --------------------------------------------------------------------------- #
# App router
# --------------------------------------------------------------------------- #
PAGES = {"🛰️ Intelligence Dashboard": page_dashboard,
         "📜 Timeline Explorer": page_timeline,
         "🕸️ Knowledge Graph Explorer": page_graph,
         "🔎 Investigation Assistant": page_assistant,
         "ℹ️ About ELTR": page_about}


def main():
    with st.sidebar:
        st.markdown("## 🛰️ ECHO-X")
        st.caption("Temporal Intelligence &\nForgotten History Reconstruction Engine")
        st.markdown("*Connecting fragmented memories to reconstruct historical truth.*")
        st.divider()
        page = st.radio("Navigate", list(PAGES), label_visibility="collapsed")
        st.divider()
        st.caption(f"catalog `{CATALOG}`\nschema `{SCHEMA}`")
        st.caption("🔒 read-only over Gold")
    PAGES[page]()


main()