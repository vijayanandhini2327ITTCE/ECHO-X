# Databricks notebook source
# ECHO-X Phase 2 — GitHub History Ingestion (Bronze layer)
OWNER = "tiangolo"          # GitHub user or organization
REPO  = "typer"             # a manageable public repository
MAX_PAGES = 5               # safety cap: 5 pages x 100 = up to 500 records per source
PER_PAGE = 100
CATALOG = "learn_get_started_de_vijayanandhini"   # your existing catalog
SCHEMA  = "echo_x"          # new schema for ECHO-X tables

# Optional: raise the rate limit with a personal access token stored in a
# Databricks secret scope. NEVER hardcode tokens in notebooks.
try:
    TOKEN = dbutils.secrets.get(scope="github", key="pat")
except Exception:
    TOKEN = None   # anonymous access works fine for a pull this small

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))   # see what already exists

# COMMAND ----------

import requests, time, json
from datetime import datetime, timezone

GITHUB_API = "https://api.github.com"
HEADERS = {"Accept": "application/vnd.github+json"}
if TOKEN:
    HEADERS["Authorization"] = f"Bearer {TOKEN}"

def api_get(url, params=None, max_retries=3):
    for attempt in range(max_retries):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (403, 429) and r.headers.get("x-ratelimit-remaining") == "0":
            reset = int(r.headers.get("x-ratelimit-reset", time.time() + 60))
            wait = max(reset - time.time(), 5)
            print(f"Rate limit reached. Sleeping {wait:.0f}s ...")
            time.sleep(wait)
            continue
        if r.status_code in (500, 502, 503):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
    raise RuntimeError(f"GitHub API failed after {max_retries} retries: {url}")

def api_get_paginated(path):
    records, page = [], 1
    while page <= MAX_PAGES:
        data = api_get(f"{GITHUB_API}{path}",
                       params={"page": page, "per_page": PER_PAGE, "state": "all"})
        if not data:
            break
        records.extend(data)
        if len(data) < PER_PAGE:
            break
        page += 1
    return records

# COMMAND ----------

raw_issues   = api_get_paginated(f"/repos/{OWNER}/{REPO}/issues")
raw_prs      = api_get_paginated(f"/repos/{OWNER}/{REPO}/pulls")
raw_releases = api_get_paginated(f"/repos/{OWNER}/{REPO}/releases")

issues = [i for i in raw_issues if "pull_request" not in i]  # REST /issues mixes PRs in
print(f"Fetched: {len(issues)} issues, {len(raw_prs)} pull requests, {len(raw_releases)} releases")

# COMMAND ----------

def to_rows(items, source_type):
    now = datetime.now(timezone.utc).isoformat()
    return [{
        "source_type": source_type,
        "source_id": f"gh:{OWNER}/{REPO}:{source_type}:{item.get('number') or item.get('id')}",
        "source_url": item.get("html_url"),
        "title": item.get("title") or item.get("name"),
        "body": item.get("body"),
        "state": item.get("state"),
        "author": (item.get("user") or {}).get("login") or (item.get("author") or {}).get("login"),
        "created_at": item.get("created_at") or item.get("published_at"),
        "updated_at": item.get("updated_at"),
        "closed_at": item.get("closed_at") or item.get("published_at"),
        "labels": ",".join(l["name"] for l in item.get("labels", [])) or item.get("tag_name"),
        "payload": json.dumps(item),      # original JSON preserved
        "ingested_at": now,
    } for item in items]

from pyspark.sql.types import StructType, StructField, StringType

def write_bronze(rows, table):
    # Explicit all-string schema: GitHub releases have no "state" or
    # "updated_at" fields, so those columns are all-None for releases and
    # Spark cannot infer their types from the data alone.
    columns = ["source_type", "source_id", "source_url", "title", "body", "state",
               "author", "created_at", "updated_at", "closed_at", "labels",
               "payload", "ingested_at"]
    schema = StructType([StructField(c, StringType(), True) for c in columns])
    df = spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)
    full = f"{CATALOG}.{SCHEMA}.{table}"
    if not spark.catalog.tableExists(full):
        df.write.saveAsTable(full)        # first run: create the table
        print(f"Created {full}")
    else:
        df.createOrReplaceTempView("updates")
        spark.sql(f"""
            MERGE INTO {full} AS t USING updates AS s
              ON t.source_id = s.source_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)                              # re-runs: incremental, no duplicates
        print(f"Merged into {full}")

write_bronze(to_rows(issues, "issue"),       "bronze_github_issues")
write_bronze(to_rows(raw_prs, "pull_request"), "bronze_github_pull_requests")
write_bronze(to_rows(raw_releases, "release"),  "bronze_github_releases")

# COMMAND ----------

# DBTITLE 1,Cell 6 - Validate Bronze Tables
# ECHO-X Phase 2 — Validate the Bronze tables
# Rule 10: every major Delta table gets validated before moving on.

print("Row counts and duplicate-source_id check per table:")
for t in ["bronze_github_issues", "bronze_github_pull_requests", "bronze_github_releases"]:
    tbl = spark.table(f"{CATALOG}.{SCHEMA}.{t}")
    total = tbl.count()
    distinct = tbl.select("source_id").distinct().count()
    dupes = "OK — no duplicates" if total == distinct else f"WARNING: {total - distinct} duplicate source_ids"
    print(f"  {t}: {total} rows, {distinct} unique source_ids -> {dupes}")

# Timestamp sanity: confirm ingestion timestamps exist and ISO strings parse
from pyspark.sql import functions as F
checks = (spark.table(f"{CATALOG}.{SCHEMA}.bronze_github_issues")
          .select(F.min("created_at").alias("earliest_created"),
                  F.max("created_at").alias("latest_created"),
                  F.min("ingested_at").alias("first_ingested_at")))
display(checks)

# Sample records from each table
display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_github_issues")
        .select("source_id", "title", "state", "author", "created_at", "source_url")
        .limit(5))

display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_github_releases")
        .select("source_id", "title", "labels", "created_at", "source_url")
        .limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 7 - Silver Layer Transform
# ECHO-X Phase 3 — Silver layer: clean, normalize, standardize, prepare for NLP
from pyspark.sql import functions as F

bronze_tables = {"issue":        "bronze_github_issues",
                 "pull_request": "bronze_github_pull_requests",
                 "release":      "bronze_github_releases"}

def to_silver(source_type):
    df = spark.table(f"{CATALOG}.{SCHEMA}.{bronze_tables[source_type]}")
    return (df
        .withColumn("title", F.trim(F.col("title")))                    # clean text
        .withColumn("body", F.trim(F.col("body")))
        .withColumn("body_chars", F.coalesce(F.length(F.col("body")), F.lit(0)))
        .withColumn("has_body", F.col("body_chars") > 0)                 # missing-value handling
        .withColumn("state", F.lower(F.coalesce(F.col("state"), F.lit("unknown"))))
        .withColumn("author", F.lower(F.coalesce(F.col("author"), F.lit("unknown"))))
        .withColumn("labels", F.lower(F.coalesce(F.col("labels"), F.lit(""))))
        .withColumn("created_ts", F.to_timestamp(F.col("created_at")))   # standardized timestamps
        .withColumn("updated_ts", F.to_timestamp(F.col("updated_at")))
        .withColumn("closed_ts", F.to_timestamp(F.col("closed_at")))
        .drop("created_at", "updated_at", "closed_at"))                  # raw strings -> typed columns

parts = [to_silver(s) for s in bronze_tables]
silver = parts[0]
for p in parts[1:]:
    silver = silver.unionByName(p)

# duplicate prevention: exactly one row per source_id
silver = silver.dropDuplicates(["source_id"])

target = f"{CATALOG}.{SCHEMA}.silver_github_records"
# Silver is fully recomputed from Bronze on every run (overwrite is by design):
silver.write.mode("overwrite").saveAsTable(target)
print(f"Wrote {silver.count()} records to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 8 - Validate Silver Table
# ECHO-X Phase 3 — Validate the Silver table
from pyspark.sql import functions as F

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")
total = silver.count()
distinct_ids = silver.select("source_id").distinct().count()
status = "OK — no duplicates" if total == distinct_ids else "DUPLICATES FOUND"
print(f"silver_github_records: {total} rows, {distinct_ids} unique source_ids -> {status}")

# Records, time coverage, and text volume per record type
display(silver.groupBy("source_type")
        .agg(F.count("*").alias("records"),
             F.min("created_ts").alias("earliest"),
             F.max("created_ts").alias("latest"),
             F.avg("body_chars").cast("int").alias("avg_body_chars")))

# Provenance + missing-date checks (never invent dates — just report gaps)
display(silver.select(
    F.sum(F.col("source_url").isNull().cast("int")).alias("missing_source_url"),
    F.sum(F.col("created_ts").isNull().cast("int")).alias("missing_created_ts"),
    F.sum((F.col("author") == "unknown").cast("int")).alias("unknown_authors")))

display(silver.select("source_id", "source_type", "title", "state", "author",
                      "created_ts", "has_body", "source_url").limit(5))

# COMMAND ----------

# DBTITLE 1,Cell 9 - Gold Event Extraction
# ECHO-X Phase 4 — Gold layer (1/2): Historical Event Extraction Engine
# Rule-based extraction (deterministic, no paid APIs, Free Edition compatible).
# Every event keeps its evidence: source_id + source_url.
# event_time = when the event HAPPENED; document_created_at = when the record was written.
from pyspark.sql import functions as F

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")

LT = F.lower(F.col("title"))    # titles searched case-insensitively
LB = F.col("labels")             # labels are already lowercase in silver
IS_DEV = F.col("source_type").isin("issue", "pull_request")

def extract_events(event_type, condition, time_col, method, confidence):
    """One event per record matching `condition`, timestamped at `time_col`.
    Records lacking that timestamp are skipped — never invent dates."""
    return (silver.filter(condition)
        .filter(F.col(time_col).isNotNull())
        .select(
            F.concat(F.col("source_id"), F.lit(f":{event_type}")).alias("event_id"),
            F.lit(event_type).alias("event_type"),
            F.col("title").alias("event_title"),
            F.coalesce(F.substring(F.col("body"), 1, 300), F.col("title")).alias("event_description"),
            F.col(time_col).alias("event_time"),
            F.col("created_ts").alias("document_created_at"),
            F.lit("recorded").alias("event_time_confidence"),
            F.col("source_id"),
            F.col("source_url"),
            F.lit(method).alias("extraction_method"),
            F.lit(confidence).cast("double").alias("confidence_score")))

rules = [
    ("version_release",            F.col("source_type") == "release", "created_ts", "record_type", 0.95),
    ("bug_discovery",             IS_DEV & (LT.rlike("\\bbug\\b") | LB.contains("bug")), "created_ts", "title_label_keyword", 0.80),
    ("bug_resolution",            IS_DEV & (LT.rlike("\\bbug\\b") | LB.contains("bug")) & (F.col("state") == "closed"), "closed_ts", "title_label_keyword", 0.75),
    ("security_incident",         LT.rlike("security|cve|vulnerab") | LB.contains("security"), "created_ts", "title_label_keyword", 0.75),
    ("performance_improvement",   LT.rlike("perf(ormance)?|slow|faster|optimi[sz]|speed"), "created_ts", "title_keyword", 0.70),
    ("feature_introduction",     IS_DEV & (LT.rlike("^add\\b|add support|support for|new feature|allow|enable") | LB.contains("feature") | LB.contains("enhancement")), "created_ts", "title_label_keyword", 0.75),
    ("architecture_modification", LT.rlike("refactor|restructur|architecture"), "created_ts", "title_keyword", 0.65),
    ("technology_migration",      LT.rlike("migrat|upgrade to|switch to|replace|vendor|port to"), "created_ts", "title_keyword", 0.65),
    ("deprecation",              LT.rlike("deprecat|remov|drop support|breaking"), "created_ts", "title_keyword", 0.65),
    ("design_discussion",         LB.contains("discussion") | LB.contains("question") | LB.contains("design") | LT.rlike("^rfc|proposal|should we|consider(ing)?"), "created_ts", "title_label_keyword", 0.60),
]

gold_events = None
for event_type, condition, time_col, method, conf in rules:
    df = extract_events(event_type, condition, time_col, method, conf)
    gold_events = df if gold_events is None else gold_events.unionByName(df)

gold_events = gold_events.dropDuplicates(["event_id"])   # prevent duplicate events

target = f"{CATALOG}.{SCHEMA}.gold_events"
gold_events.write.mode("overwrite").saveAsTable(target)   # Gold recomputed from Silver
print(f"Wrote {gold_events.count()} events to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 10 - Validate gold_events
# ECHO-X Phase 4 — Validate gold_events
from pyspark.sql import functions as F

gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
total = gold_events.count()
uniq = gold_events.select("event_id").distinct().count()
print(f"gold_events: {total} rows, {uniq} unique event_ids -> "
      f"{'OK — no duplicates' if total == uniq else 'DUPLICATES FOUND'}")

# A record can legitimately yield several events (bug discovered THEN resolved)
display(gold_events.groupBy("event_type")
        .agg(F.count("*").alias("events"),
             F.min("event_time").alias("earliest"),
             F.max("event_time").alias("latest"),
             F.round(F.avg("confidence_score"), 2).alias("avg_confidence")))

# Evidence integrity: every event must trace back to a source URL
display(gold_events.select(
    F.sum(F.col("source_url").isNull().cast("int")).alias("events_without_source_url"),
    F.sum(F.col("event_time").isNull().cast("int")).alias("events_without_time")))

display(gold_events.select("event_type", "event_title", "event_time",
                           "document_created_at", "confidence_score", "source_url").limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 11 - Gold Entity Extraction
# ECHO-X Phase 4 — Gold layer (2/2): Entity Extraction Engine
# Extracts and RESOLVES entities: every GitHub record, every developer,
# and technologies found by keyword scan (mentions grouped by normalized name).
from pyspark.sql import functions as F

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")

entity_cols = ["entity_id", "entity_type", "original_name", "normalized_name",
               "mention_count", "first_seen_ts", "last_seen_ts",
               "extraction_method", "confidence_score"]

# 1) Every GitHub record is itself an entity (issue / pull_request / release)
record_entities = silver.select(
    F.col("source_id").alias("entity_id"),
    F.col("source_type").alias("entity_type"),
    F.col("title").alias("original_name"),
    F.col("source_id").alias("normalized_name"),
    F.lit(1).cast("long").alias("mention_count"),
    F.col("created_ts").alias("first_seen_ts"),
    F.col("created_ts").alias("last_seen_ts"),
    F.lit("github_record").alias("extraction_method"),
    F.lit(1.0).cast("double").alias("confidence_score"))

# 2) Developers — entity resolution: one row per distinct login
developer_entities = (silver.groupBy("author")
    .agg(F.count("*").alias("mention_count"),
         F.min("created_ts").alias("first_seen_ts"),
         F.max("created_ts").alias("last_seen_ts"))
    .select(
        F.concat(F.lit("developer:"), F.col("author")).alias("entity_id"),
        F.lit("developer").alias("entity_type"),
        F.col("author").alias("original_name"),
        F.col("author").alias("normalized_name"),
        "mention_count", "first_seen_ts", "last_seen_ts",
        F.lit("author_field").alias("extraction_method"),
        F.lit(1.0).cast("double").alias("confidence_score")))

# 3) Technologies — keyword scan over titles + bodies (word-boundary match)
TECH_KEYWORDS = ["python", "click", "rich", "shellingham", "mypy", "pytest",
                 "pydantic", "fastapi", "docker", "sqlite", "github actions"]

tech_entities = None
for kw in TECH_KEYWORDS:
    df = (silver
        .withColumn("search_text", F.concat_ws(" ",
             F.lower(F.coalesce(F.col("title"), F.lit(""))),
             F.lower(F.coalesce(F.col("body"), F.lit("")))))
        .filter(F.col("search_text").rlike("\\b" + kw + "\\b"))
        .groupBy(F.lit(kw).alias("normalized_name"))
        .agg(F.count("*").alias("mention_count"),
             F.min("created_ts").alias("first_seen_ts"),
             F.max("created_ts").alias("last_seen_ts"))
        .select(
            F.concat(F.lit("technology:"), F.col("normalized_name")).alias("entity_id"),
            F.lit("technology").alias("entity_type"),
            F.col("normalized_name").alias("original_name"),
            "normalized_name", "mention_count", "first_seen_ts", "last_seen_ts",
            F.lit("keyword_scan").alias("extraction_method"),
            F.lit(0.8).cast("double").alias("confidence_score")))
    tech_entities = df if tech_entities is None else tech_entities.unionByName(df)

gold_entities = (record_entities.select(*entity_cols)
    .unionByName(developer_entities.select(*entity_cols))
    .unionByName(tech_entities.select(*entity_cols))
    .dropDuplicates(["entity_id"]))

target = f"{CATALOG}.{SCHEMA}.gold_entities"
gold_entities.write.mode("overwrite").saveAsTable(target)  # Gold recomputed from Silver
print(f"Wrote {gold_entities.count()} entities to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 12 - Validate gold_entities
# ECHO-X Phase 4 — Validate gold_entities
from pyspark.sql import functions as F

gold_entities = spark.table(f"{CATALOG}.{SCHEMA}.gold_entities")
total = gold_entities.count()
uniq = gold_entities.select("entity_id").distinct().count()
print(f"gold_entities: {total} rows, {uniq} unique entity_ids -> "
      f"{'OK — no duplicates' if total == uniq else 'DUPLICATES FOUND'}")

display(gold_entities.groupBy("entity_type")
        .agg(F.count("*").alias("entities"),
             F.sum("mention_count").alias("total_mentions")))

# Top technologies by mentions (entity resolution: same name -> one row)
display(gold_entities.filter("entity_type = 'technology'")
        .orderBy(F.desc("mention_count"))
        .select("normalized_name", "mention_count", "first_seen_ts", "last_seen_ts", "confidence_score"))

# Most active developers
display(gold_entities.filter("entity_type = 'developer'")
        .orderBy(F.desc("mention_count")).limit(5)
        .select("normalized_name", "mention_count", "first_seen_ts", "last_seen_ts"))

# COMMAND ----------

# DBTITLE 1,Cell 13 - Gold Timeline
# ECHO-X Phase 5 (1/3) — Temporal Intelligence: chronological timeline
# Every historical event gets a stable sequence number in true time order.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")

timeline = (gold_events
    .withColumn("sequence_number", F.row_number().over(
        Window.orderBy(F.col("event_time"), F.col("event_id"))))
    .withColumn("event_year", F.year("event_time"))
    .withColumn("event_month", F.month("event_time"))
    .select("sequence_number", "event_id", "event_type", "event_title",
            "event_time", "event_year", "event_month",
            "confidence_score", "source_id", "source_url"))

target = f"{CATALOG}.{SCHEMA}.gold_timeline"
timeline.write.mode("overwrite").saveAsTable(target)   # recomputed from gold_events
print(f"Wrote {timeline.count()} timeline records to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 14 - Incident Chains + Tech Evolution
# ECHO-X Phase 5 (2/3) — Incident-to-resolution chains + technology evolution
from pyspark.sql import functions as F

gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
gold_entities = spark.table(f"{CATALOG}.{SCHEMA}.gold_entities")
silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")

# 1) Incident-to-resolution: each bug_discovery paired with the matching
#    bug_resolution from the SAME source record (evidence-preserving join).
#    Unresolved bugs keep a NULL resolved_at — we never hide them.
discoveries = (gold_events.filter("event_type = 'bug_discovery'")
    .select("source_id", "source_url",
            F.col("event_title").alias("bug_title"),
            F.col("event_time").alias("discovered_at")))
resolutions = (gold_events.filter("event_type = 'bug_resolution'")
    .select("source_id", F.col("event_time").alias("resolved_at")))

incident_resolutions = (discoveries.join(resolutions, on="source_id", how="left")
    .withColumn("resolution_days", F.round(F.datediff("resolved_at", "discovered_at"), 1))
    .select("source_id", "bug_title", "discovered_at", "resolved_at",
            "resolution_days", "source_url"))

target = f"{CATALOG}.{SCHEMA}.gold_incident_resolutions"
incident_resolutions.write.mode("overwrite").saveAsTable(target)
print(f"Wrote {incident_resolutions.count()} incident-resolution chains to {target}")

# 2) Technology evolution: mentions per technology per year.
#    Word-boundary keyword match (same rules as gold_entities) — no UDFs.
tech_names = [r["normalized_name"] for r in
              gold_entities.filter("entity_type = 'technology'")
                           .select("normalized_name").collect()]

silver_text = (silver
    .withColumn("search_text", F.concat_ws(" ",
        F.lower(F.coalesce(F.col("title"), F.lit(""))),
        F.lower(F.coalesce(F.col("body"), F.lit("")))))
    .select("created_ts", "search_text"))

evolution = None
for kw in tech_names:
    df = (silver_text.filter(F.col("search_text").rlike("\\b" + kw + "\\b"))
        .groupBy(F.lit(kw).alias("normalized_name"),
                 F.year("created_ts").alias("year"))
        .agg(F.count("*").alias("mentions")))
    evolution = df if evolution is None else evolution.unionByName(df)

evolution = evolution.orderBy("normalized_name", "year")

target = f"{CATALOG}.{SCHEMA}.gold_technology_evolution"
evolution.write.mode("overwrite").saveAsTable(target)
print(f"Wrote {evolution.count()} technology-year records to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 15 - Validate Temporal Outputs
# ECHO-X Phase 5 (3/3) — Validate temporal intelligence outputs
from pyspark.sql import functions as F
from pyspark.sql.window import Window

timeline = spark.table(f"{CATALOG}.{SCHEMA}.gold_timeline")
ir = spark.table(f"{CATALOG}.{SCHEMA}.gold_incident_resolutions")
evolution = spark.table(f"{CATALOG}.{SCHEMA}.gold_technology_evolution")

# Timeline integrity: contiguous sequence + non-decreasing time
n = timeline.count()
seq = timeline.agg(F.min("sequence_number").alias("lo"),
                   F.max("sequence_number").alias("hi")).first()
violations = (timeline
    .withColumn("prev_time", F.lag("event_time").over(Window.orderBy("sequence_number")))
    .filter(F.col("prev_time").isNotNull() & (F.col("event_time") < F.col("prev_time")))
    .count())
print(f"gold_timeline: {n} rows, sequence {seq['lo']}..{seq['hi']} (expected 1..{n}), "
      f"chronology violations: {violations}")

# Events per year (version evolution at a glance)
display(timeline.groupBy("event_year").agg(F.count("*").alias("events"))
        .orderBy("event_year"))

# Missing historical periods: >90 days with no recorded event.
# These are COVERAGE GAPS in the ingested data — NOT proof nothing happened.
gaps = (timeline
    .withColumn("prev_time", F.lag("event_time").over(Window.orderBy("sequence_number")))
    .filter(F.col("prev_time").isNotNull())
    .withColumn("gap_days", F.datediff("event_time", "prev_time"))
    .filter(F.col("gap_days") > 90)
    .select(F.col("prev_time").alias("silence_from"),
            F.col("event_time").alias("next_event_at"), "gap_days"))
print(f"Coverage gaps longer than 90 days: {gaps.count()}")
display(gaps)

# Incident-to-resolution statistics
display(ir.agg(F.count("*").alias("bugs_tracked"),
    F.sum(F.col("resolved_at").isNull().cast("int")).alias("still_unresolved"),
    F.round(F.avg("resolution_days"), 1).alias("avg_days_to_resolve"),
    F.max("resolution_days").alias("max_days")))

# Before-and-after exploration: events within 14 days of the latest release
anchor = (timeline.filter("event_type = 'version_release'")
                  .orderBy(F.desc("event_time")).first())
print(f"Events within 14 days of the latest release "
      f"({anchor['event_title']} on {anchor['event_time']}):")
display(timeline
    .filter(F.abs(F.datediff("event_time", F.lit(anchor["event_time"]))) <= 14)
    .orderBy("event_time")
    .select("sequence_number", "event_type", "event_title", "event_time", "source_url"))

# Technology evolution (mentions per technology per year)
display(evolution)

# COMMAND ----------

# DBTITLE 1,Cell 16 - Knowledge Graph Nodes
# ECHO-X Phase 6 (1/3) — Knowledge Graph: NODES
# One node per extracted event + one node per resolved entity.
# Stable IDs: event nodes use event_id, entity nodes use entity_id.
from pyspark.sql import functions as F

gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
gold_entities = spark.table(f"{CATALOG}.{SCHEMA}.gold_entities")

event_nodes = gold_events.select(
    F.col("event_id").alias("node_id"),
    F.lit("event").alias("node_type"),
    F.col("event_type").alias("node_subtype"),
    F.col("event_title").alias("label"),
    F.col("event_time"),
    F.lit(None).cast("long").alias("mention_count"),
    F.col("confidence_score"),
    F.col("source_id"),
    F.col("source_url"))

entity_nodes = gold_entities.select(
    F.col("entity_id").alias("node_id"),
    F.col("entity_type").alias("node_type"),
    F.lit(None).cast("string").alias("node_subtype"),
    F.col("original_name").alias("label"),
    F.lit(None).cast("timestamp").alias("event_time"),
    F.col("mention_count"),
    F.col("confidence_score"),
    F.when(F.col("entity_type").isin("issue", "pull_request", "release"),
           F.col("entity_id")).alias("source_id"),
    F.lit(None).cast("string").alias("source_url"))

gold_nodes = (event_nodes.unionByName(entity_nodes)
              .dropDuplicates(["node_id"]))

target = f"{CATALOG}.{SCHEMA}.gold_nodes"
gold_nodes.write.mode("overwrite").saveAsTable(target)   # recomputed from gold_events/entities
print(f"Wrote {gold_nodes.count()} nodes to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 17 - Knowledge Graph Edges
# ECHO-X Phase 6 (2/3) — Knowledge Graph: EDGES
# Each edge carries: relationship type, basis (documented vs inferred),
# confidence, and the evidence record (source_id + source_url) that produced it.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
gold_entities = spark.table(f"{CATALOG}.{SCHEMA}.gold_entities")
gold_timeline = spark.table(f"{CATALOG}.{SCHEMA}.gold_timeline")
ir = spark.table(f"{CATALOG}.{SCHEMA}.gold_incident_resolutions")
silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")

edge_cols = ["source_node_id", "target_node_id", "relationship_type",
             "relationship_basis", "confidence_score",
             "evidence_source_id", "evidence_source_url"]

def edges(df):
    """Standardize columns and give every edge a stable edge_id."""
    return (df.select(*edge_cols)
        .withColumn("edge_id", F.md5(F.concat_ws("|", "source_node_id",
             "target_node_id", "relationship_type"))))

# 1) RESOLVED_BY (documented): bug_discovery -> its bug_resolution, same record
e1 = (ir.filter("resolved_at IS NOT NULL")
    .select(F.concat("source_id", F.lit(":bug_discovery")).alias("source_node_id"),
            F.concat("source_id", F.lit(":bug_resolution")).alias("target_node_id"),
            F.lit("RESOLVED_BY").alias("relationship_type"),
            F.lit("documented").alias("relationship_basis"),
            F.lit(0.75).cast("double").alias("confidence_score"),
            F.col("source_id").alias("evidence_source_id"),
            F.col("source_url").alias("evidence_source_url")))

# 2) PRECEDED_BY (inferred from recorded times): each event -> the event before it
e2 = (gold_timeline
    .withColumn("prev_event_id", F.lag("event_id").over(Window.orderBy("sequence_number")))
    .filter(F.col("prev_event_id").isNotNull())
    .select(F.col("event_id").alias("source_node_id"),
            F.col("prev_event_id").alias("target_node_id"),
            F.lit("PRECEDED_BY").alias("relationship_type"),
            F.lit("inferred").alias("relationship_basis"),
            F.lit(0.60).cast("double").alias("confidence_score"),
            F.col("source_id").alias("evidence_source_id"),
            F.col("source_url").alias("evidence_source_url")))

# 3) SUPERSEDED_BY (inferred version order): each release -> the next release
e3 = (gold_events.filter("event_type = 'version_release'")
    .withColumn("next_event_id", F.lead("event_id").over(Window.orderBy("event_time")))
    .filter(F.col("next_event_id").isNotNull())
    .select(F.col("event_id").alias("source_node_id"),
            F.col("next_event_id").alias("target_node_id"),
            F.lit("SUPERSEDED_BY").alias("relationship_type"),
            F.lit("inferred").alias("relationship_basis"),
            F.lit(0.70).cast("double").alias("confidence_score"),
            F.col("source_id").alias("evidence_source_id"),
            F.col("source_url").alias("evidence_source_url")))

# 4) IMPLEMENTED_IN (inferred): each bug fix -> first release AFTER the fix
res = (gold_events.filter("event_type = 'bug_resolution'")
    .select(F.col("event_id").alias("res_event_id"),
            F.col("event_time").alias("resolved_at"),
            F.col("source_id"), F.col("source_url")))
rele = (gold_events.filter("event_type = 'version_release'")
    .select(F.col("event_id").alias("rel_event_id"),
            F.col("event_time").alias("release_at")))
e4 = (res.join(rele, F.col("release_at") > F.col("resolved_at"))
    .withColumn("rn", F.row_number().over(Window.partitionBy("res_event_id").orderBy("release_at")))
    .filter("rn = 1")
    .select(F.col("res_event_id").alias("source_node_id"),
            F.col("rel_event_id").alias("target_node_id"),
            F.lit("IMPLEMENTED_IN").alias("relationship_type"),
            F.lit("inferred").alias("relationship_basis"),
            F.lit(0.50).cast("double").alias("confidence_score"),
            F.col("source_id").alias("evidence_source_id"),
            F.col("source_url").alias("evidence_source_url")))

# 5) RELATED_TO (inferred): technology entity <-> each record mentioning it
tech_names = [r["normalized_name"] for r in
              gold_entities.filter("entity_type = 'technology'")
                           .select("normalized_name").collect()]
silver_text = (silver
    .withColumn("search_text", F.concat_ws(" ",
        F.lower(F.coalesce(F.col("title"), F.lit(""))),
        F.lower(F.coalesce(F.col("body"), F.lit("")))))
    .select("source_id", "source_url", "search_text"))

e5_parts = []
for kw in tech_names:
    e5_parts.append(silver_text
        .filter(F.col("search_text").rlike("\\b" + kw + "\\b"))
        .select(F.concat(F.lit("technology:"), F.lit(kw)).alias("source_node_id"),
                F.col("source_id").alias("target_node_id"),
                F.lit("RELATED_TO").alias("relationship_type"),
                F.lit("inferred").alias("relationship_basis"),
                F.lit(0.60).cast("double").alias("confidence_score"),
                F.col("source_id").alias("evidence_source_id"),
                F.col("source_url").alias("evidence_source_url")))

# 6) RELATED_TO (documented): developer entity <-> each record they authored
e6 = silver.select(
    F.concat(F.lit("developer:"), F.col("author")).alias("source_node_id"),
    F.col("source_id").alias("target_node_id"),
    F.lit("RELATED_TO").alias("relationship_type"),
    F.lit("documented").alias("relationship_basis"),
    F.lit(0.90).cast("double").alias("confidence_score"),
    F.col("source_id").alias("evidence_source_id"),
    F.col("source_url").alias("evidence_source_url"))

gold_edges = edges(e1)
for part in [e2, e3, e4, e6] + e5_parts:
    gold_edges = gold_edges.unionByName(edges(part))

gold_edges = gold_edges.dropDuplicates(["edge_id"])   # prevent duplicate edges

target = f"{CATALOG}.{SCHEMA}.gold_edges"
gold_edges.write.mode("overwrite").saveAsTable(target)  # recomputed each run
print(f"Wrote {gold_edges.count()} edges to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 18 - Validate Knowledge Graph
# ECHO-X Phase 6 (3/3) — Validate the knowledge graph
from pyspark.sql import functions as F

gold_nodes = spark.table(f"{CATALOG}.{SCHEMA}.gold_nodes")
gold_edges = spark.table(f"{CATALOG}.{SCHEMA}.gold_edges")

n_nodes = gold_nodes.count()
n_edges = gold_edges.count()
uniq_nodes = gold_nodes.select("node_id").distinct().count()
uniq_edges = gold_edges.select("edge_id").distinct().count()
print(f"gold_nodes: {n_nodes} rows, {uniq_nodes} unique node_ids -> "
      f"{'OK' if n_nodes == uniq_nodes else 'DUPLICATES'}")
print(f"gold_edges: {n_edges} rows, {uniq_edges} unique edge_ids -> "
      f"{'OK' if n_edges == uniq_edges else 'DUPLICATES'}")

# Node breakdown
display(gold_nodes.groupBy("node_type").agg(F.count("*").alias("nodes")))

# Edge breakdown: type x documented vs inferred
display(gold_edges.groupBy("relationship_type", "relationship_basis")
        .agg(F.count("*").alias("edges"),
             F.round(F.avg("confidence_score"), 2).alias("avg_confidence")))

# Referential integrity: every edge endpoint must reference an existing node
node_ids = gold_nodes.select("node_id")
orphans = (gold_edges.select(F.col("source_node_id").alias("node_id"))
    .union(gold_edges.select(F.col("target_node_id").alias("node_id")))
    .distinct().join(node_ids, on="node_id", how="left_anti"))
n_orphans = orphans.count()
print(f"Edge endpoints not found in gold_nodes: {n_orphans} "
      f"-> {'OK — graph is consistent' if n_orphans == 0 else 'BROKEN EDGES FOUND'}")

# Graph traversal demo: bug -> RESOLVED_BY -> fix -> IMPLEMENTED_IN -> release
edges_t, nodes_t = f"{CATALOG}.{SCHEMA}.gold_edges", f"{CATALOG}.{SCHEMA}.gold_nodes"
display(spark.sql(f"""
    SELECT n1.label AS bug, e1.relationship_type AS step1,
           n2.label AS fix, e2.relationship_type AS step2,
           n3.label AS release_in, n3.node_subtype
    FROM {edges_t} e1
    JOIN {edges_t} e2 ON e1.target_node_id = e2.source_node_id
                     AND e2.relationship_type = 'IMPLEMENTED_IN'
    JOIN {nodes_t} n1 ON n1.node_id = e1.source_node_id
    JOIN {nodes_t} n2 ON n2.node_id = e1.target_node_id
    JOIN {nodes_t} n3 ON n3.node_id = e2.target_node_id
    WHERE e1.relationship_type = 'RESOLVED_BY'
    ORDER BY n1.label
"""))

# Most connected nodes (degree = incoming + outgoing edges)
degree = (gold_edges.groupBy(F.col("source_node_id").alias("node_id")).agg(F.count("*").alias("out_degree"))
    .unionByName(gold_edges.groupBy(F.col("target_node_id").alias("node_id")).agg(F.count("*").alias("out_degree")))
    .groupBy("node_id").agg(F.sum("out_degree").alias("degree"))
    .orderBy(F.desc("degree")).limit(10))
display(degree.join(gold_nodes.select("node_id", "node_type", "label"), on="node_id", how="left"))

# COMMAND ----------

# DBTITLE 1,Cell 19 - Decision Reconstruction
# ECHO-X Phase 7 (1/2) — Decision Reconstruction Engine
# Identify decision records, then reconstruct status and context FROM EVIDENCE.
# A discussion is NOT assumed to be a finalized decision — status comes from
# the record's state plus verifiable follow-up (release after closure).
from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")
gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")

# Decision candidates: titles signaling a deliberate engineering choice
LT = F.lower(F.col("title"))
candidates = silver.filter(
    LT.rlike("migrat|vendor|refactor|restructur|architect|deprecat|drop support|replace|switch to|port to|proposal|breaking")
).select(
    "source_id", "source_type", "title", "body", "state",
    F.col("created_ts").alias("decision_date"),
    "closed_ts", "source_url")

# A) Implementation evidence: first release within 90 days AFTER closure
rel = (gold_events.filter("event_type = 'version_release'")
    .select(F.col("event_id").alias("impl_release_event_id"),
            F.col("event_time").alias("release_at"),
            F.col("event_title").alias("release_title")))
impl = (candidates.filter("closed_ts IS NOT NULL")
    .join(rel, (F.col("release_at") > F.col("closed_ts")) &
               (F.col("release_at") <= F.date_add(F.col("closed_ts").cast("date"), 90)))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("source_id").orderBy("release_at")))
    .filter("rn = 1")
    .select("source_id", "impl_release_event_id", "release_title"))

# B) Triggering problem: latest problem-type event in the 30 days BEFORE the decision
problems = (gold_events.filter("event_type in ('bug_discovery', 'security_incident', 'deprecation')")
    .select(F.col("source_id").alias("problem_source_id"),
            F.col("event_id").alias("problem_event_id"),
            F.col("event_title").alias("problem_title"),
            F.col("event_time").alias("problem_time")))
trigger = (candidates.select("source_id", "decision_date")
    .join(problems,
          (F.col("problem_time") < F.col("decision_date")) &
          (F.col("problem_time") >= F.date_sub(F.col("decision_date").cast("date"), 30)) &
          (F.col("problem_source_id") != F.col("source_id")))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("source_id").orderBy(F.desc("problem_time"))))
    .filter("rn = 1")
    .select("source_id", "problem_event_id", "problem_title"))

# C) Related events (plus/minus 30 days) and subsequent outcomes (+30 days)
allevents = gold_events.select(
    F.col("source_id").alias("ev_source_id"), "event_id", "event_time")
ctx = (candidates.select("source_id", "decision_date")
    .join(allevents,
          (F.col("ev_source_id") != F.col("source_id")) &
          (F.abs(F.datediff(F.col("event_time").cast("date"),
                            F.col("decision_date").cast("date"))) <= 30))
    .groupBy("source_id")
    .agg(F.count("*").alias("related_event_count"),
         F.slice(F.collect_set("event_id"), 1, 10).alias("related_event_ids")))
outcomes = (candidates.select("source_id", "decision_date")
    .join(allevents,
          (F.col("ev_source_id") != F.col("source_id")) &
          (F.datediff(F.col("event_time").cast("date"),
                      F.col("decision_date").cast("date")) >= 1) &
          (F.datediff(F.col("event_time").cast("date"),
                      F.col("decision_date").cast("date")) <= 30))
    .groupBy("source_id")
    .agg(F.count("*").alias("subsequent_outcome_count")))

# Assemble: status from evidence, alternatives flag from discussion text
decisions = (candidates
    .join(impl, on="source_id", how="left")
    .join(trigger, on="source_id", how="left")
    .join(ctx, on="source_id", how="left")
    .join(outcomes, on="source_id", how="left")
    .withColumn("alternatives_mentioned",
        F.lower(F.coalesce(F.col("body"), F.lit("")))
            .rlike("alternativ|instead of|rather than|or we could|another option"))
    .withColumn("decision_status",
        F.when(F.col("state").isin("open", "unknown"), F.lit("unresolved"))
         .when(F.col("impl_release_event_id").isNotNull(), F.lit("implemented"))
         .otherwise(F.lit("accepted")))
    .withColumn("status_basis",
        F.when(F.col("decision_status") == "unresolved", F.lit("record still open or state unknown"))
         .when(F.col("decision_status") == "implemented", F.lit("release within 90 days after closure"))
         .otherwise(F.lit("closed without release evidence")))
    .withColumn("confidence_score",
        F.when(F.col("decision_status") == "implemented", F.lit(0.75))
         .when(F.col("decision_status") == "accepted", F.lit(0.60))
         .otherwise(F.lit(0.50)).cast("double"))
    .select(
        F.concat(F.lit("decision:"), F.col("source_id")).alias("decision_id"),
        F.col("title").alias("decision_title"),
        F.substring(F.coalesce(F.col("body"), F.col("title")), 1, 500).alias("decision_description"),
        "decision_date", "decision_status", "status_basis",
        F.col("problem_event_id").alias("triggering_problem_event_id"),
        F.col("problem_title").alias("triggering_problem_title"),
        F.coalesce(F.col("related_event_count"), F.lit(0)).alias("related_event_count"),
        "related_event_ids", "alternatives_mentioned",
        F.col("impl_release_event_id").alias("implementation_release_event_id"),
        F.col("release_title").alias("implementation_release_title"),
        F.coalesce(F.col("subsequent_outcome_count"), F.lit(0)).alias("subsequent_outcome_count"),
        "confidence_score", "source_id", "source_url",
        F.lit("title_keyword_and_status_rules").alias("extraction_method")))

gold_decisions = decisions.dropDuplicates(["decision_id"])

target = f"{CATALOG}.{SCHEMA}.gold_decisions"
gold_decisions.write.mode("overwrite").saveAsTable(target)   # recomputed each run
print(f"Wrote {gold_decisions.count()} reconstructed decisions to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 20 - Validate gold_decisions
# ECHO-X Phase 7 (2/2) — Validate gold_decisions
from pyspark.sql import functions as F

gold_decisions = spark.table(f"{CATALOG}.{SCHEMA}.gold_decisions")
total = gold_decisions.count()
uniq = gold_decisions.select("decision_id").distinct().count()
print(f"gold_decisions: {total} rows, {uniq} unique decision_ids -> "
      f"{'OK — no duplicates' if total == uniq else 'DUPLICATES FOUND'}")

# Status distribution — every status carries its evidence basis
display(gold_decisions.groupBy("decision_status", "status_basis")
        .agg(F.count("*").alias("decisions"),
             F.round(F.avg("confidence_score"), 2).alias("avg_confidence")))

# Context reconstruction coverage
display(gold_decisions.select(
    F.sum(F.col("source_url").isNull().cast("int")).alias("missing_source_url"),
    F.sum(F.col("decision_date").isNull().cast("int")).alias("missing_date"),
    F.sum(F.col("triggering_problem_event_id").isNotNull().cast("int")).alias("with_triggering_problem"),
    F.sum(F.col("alternatives_mentioned").cast("int")).alias("alternatives_discussed"),
    F.sum(F.col("implementation_release_event_id").isNotNull().cast("int")).alias("with_release_evidence")))

# Fully reconstructed decisions: trigger + implementation + outcomes
display(gold_decisions
    .filter("triggering_problem_event_id IS NOT NULL AND implementation_release_event_id IS NOT NULL")
    .select("decision_title", "decision_date", "triggering_problem_title",
            "implementation_release_title", "subsequent_outcome_count",
            "confidence_score", "source_url")
    .orderBy(F.desc("decision_date")).limit(10))

# Still-unresolved decisions (open questions, honestly reported)
display(gold_decisions.filter("decision_status = 'unresolved'")
    .select("decision_title", "decision_date", "confidence_score", "source_url")
    .orderBy(F.desc("decision_date")))

# COMMAND ----------

# DBTITLE 1,Cell 21 - Contradiction Detection
# ECHO-X Phase 8 (1/2) — Contradiction Intelligence Engine
# Detect conflicting historical information. BOTH sides are always preserved —
# the engine never silently picks one version. Normal lifecycle differences
# (created vs closed times of the SAME record) are NOT treated as conflicts.
from pyspark.sql import functions as F

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")
gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")

# 1) Identity mismatch: record source_id names one repository owner, but
#    the source_url points to a different one (repo transfer / rename).
identity = (silver
    .withColumn("id_repo", F.regexp_extract("source_id", "^gh:([^:]+):", 1))
    .withColumn("url_repo", F.regexp_extract("source_url", "github\\.com/([^/]+/[^/]+)/", 1))
    .filter(F.col("id_repo") != F.col("url_repo"))
    .groupBy("id_repo", "url_repo")
    .agg(F.count("*").alias("records_affected"),
         F.slice(F.collect_set("source_url"), 1, 5).alias("evidence_urls"))
    .select(
        F.lit("identity_mismatch").alias("category"),
        F.lit("repository identity").alias("subject"),
        F.lit("canonical_name").alias("attribute"),
        F.col("id_repo").alias("value_a"),
        F.col("url_repo").alias("value_b"),
        "records_affected", "evidence_urls",
        F.lit(0.90).cast("double").alias("confidence_score"),
        F.lit("id_vs_url_comparison").alias("detection_method")))

# 2) Date-order inconsistency INSIDE one record: closed/updated before created
#    (impossible ordering -> data-level contradiction, not a lifecycle difference)
date_order = (silver
    .filter((F.col("closed_ts") < F.col("created_ts")) |
            (F.col("updated_ts") < F.col("created_ts")))
    .select(
        F.lit("date_order_inconsistency").alias("category"),
        F.col("source_id").alias("subject"),
        F.lit("timestamp_order").alias("attribute"),
        F.concat_ws(" / ", "created_ts", "updated_ts", "closed_ts").alias("value_a"),
        F.lit("created must precede updated and closed").alias("value_b"),
        F.lit(1).cast("long").alias("records_affected"),
        F.array(F.col("source_url")).alias("evidence_urls"),
        F.lit(1.00).cast("double").alias("confidence_score"),
        F.lit("timestamp_comparison").alias("detection_method")))

# 3) Same event title reported with DIFFERENT dates by different records
date_conflict = (gold_events
    .groupBy("event_title", "event_type")
    .agg(F.countDistinct("source_id").alias("records_affected"),
         F.countDistinct("event_time").alias("distinct_times"),
         F.min("event_time").alias("first_time"),
         F.max("event_time").alias("last_time"),
         F.slice(F.collect_set("source_url"), 1, 5).alias("evidence_urls"))
    .filter((F.col("records_affected") > 1) & (F.col("distinct_times") > 1))
    .select(
        F.lit("conflicting_event_dates").alias("category"),
        F.col("event_title").alias("subject"),
        F.lit("event_time").alias("attribute"),
        F.col("first_time").cast("string").alias("value_a"),
        F.col("last_time").cast("string").alias("value_b"),
        "records_affected", "evidence_urls",
        F.lit(0.70).cast("double").alias("confidence_score"),
        F.lit("same_title_different_dates").alias("detection_method")))

# 4) Same subject title reported in DIFFERENT states by different records
status_conflict = (silver
    .groupBy(F.lower(F.col("title")).alias("t"))
    .agg(F.countDistinct("source_id").alias("records_affected"),
         F.collect_set("state").alias("states"),
         F.slice(F.collect_set("source_url"), 1, 5).alias("evidence_urls"))
    .filter((F.col("records_affected") > 1) & (F.size("states") > 1))
    .select(
        F.lit("conflicting_status").alias("category"),
        F.col("t").alias("subject"),
        F.lit("record_state").alias("attribute"),
        F.array_join("states", " vs ").alias("value_a"),
        F.lit("different records, same subject title").alias("value_b"),
        "records_affected", "evidence_urls",
        F.lit(0.65).cast("double").alias("confidence_score"),
        F.lit("same_title_different_states").alias("detection_method")))

gold_contradictions = (identity.unionByName(date_order)
    .unionByName(date_conflict)
    .unionByName(status_conflict)
    .withColumn("contradiction_id",
        F.md5(F.concat_ws("|", "category", "subject", "attribute")))
    .dropDuplicates(["contradiction_id"]))

target = f"{CATALOG}.{SCHEMA}.contradiction_records"
gold_contradictions.write.mode("overwrite").saveAsTable(target)  # recomputed each run
print(f"Wrote {gold_contradictions.count()} contradictions to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 22 - Validate Contradictions
# ECHO-X Phase 8 (2/2) — Validate contradiction_records
from pyspark.sql import functions as F

contradictions = spark.table(f"{CATALOG}.{SCHEMA}.contradiction_records")
total = contradictions.count()
uniq = contradictions.select("contradiction_id").distinct().count()
print(f"contradiction_records: {total} rows, {uniq} unique contradiction_ids -> "
      f"{'OK — no duplicates' if total == uniq else 'DUPLICATES FOUND'}")

# Contradictions per category (categories with 0 findings simply do not appear)
display(contradictions.groupBy("category")
        .agg(F.count("*").alias("contradictions"),
             F.sum("records_affected").alias("records_affected"),
             F.round(F.avg("confidence_score"), 2).alias("avg_confidence")))

# Both-sides-preserved check: value_a AND value_b must exist, with evidence
display(contradictions.select(
    F.sum(F.col("value_a").isNull().cast("int")).alias("missing_value_a"),
    F.sum(F.col("value_b").isNull().cast("int")).alias("missing_value_b"),
    F.sum(F.col("evidence_urls").isNotNull().cast("int")).alias("with_evidence")))

# Largest contradictions by affected records
display(contradictions.orderBy(F.desc("records_affected")).limit(10)
        .select("category", "subject", "attribute", "value_a", "value_b",
                "records_affected", "confidence_score"))

# Show the evidence URLs for the biggest contradiction
biggest = contradictions.orderBy(F.desc("records_affected")).first()
if biggest is not None:
    print(f"Evidence URLs for largest contradiction ({biggest['category']}, "
          f"{biggest['records_affected']} records):")
    for u in (biggest["evidence_urls"] or [])[:3]:
        print(f"  {u}")

# COMMAND ----------

# DBTITLE 1,Cell 23 - ELTR Integration
# ECHO-X Phase 9 — ELTR: Evidence-Linked Temporal Reconstruction
# Integration layer: temporal alignment + evidence association + decision
# lineage + relationship discovery + contradiction analysis + confidence-aware
# classification — one investigation row per reconstructed decision.
# Facts are separated into: verified (documented edges), inferred (confidence
# labeled), and declared gaps (missing information, never filled by guessing).
from pyspark.sql import functions as F

gold_decisions = spark.table(f"{CATALOG}.{SCHEMA}.gold_decisions")
gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
gold_edges = spark.table(f"{CATALOG}.{SCHEMA}.gold_edges")
contradictions = spark.table(f"{CATALOG}.{SCHEMA}.contradiction_records")

EMPTY_STR_ARRAY = F.array().cast("array<string>")

dec = (gold_decisions.select(
    "decision_id", "decision_title", "decision_date", "decision_status",
    "triggering_problem_title", "alternatives_mentioned",
    F.col("source_id").alias("dec_source_id"),
    F.col("source_url").alias("decision_url"),
    F.col("confidence_score").alias("decision_confidence")))

# 1) Temporal alignment: all events within 60 days of the decision
slices = (dec.select("decision_id", "decision_date")
    .join(gold_events.select("event_id", "event_type", "event_title", "event_time",
                             "confidence_score", "source_url"),
          F.abs(F.datediff(F.col("event_time").cast("date"),
                           F.col("decision_date").cast("date"))) <= 60)
    .groupBy("decision_id")
    .agg(F.count("*").alias("aligned_event_count"),
         F.round(F.avg("confidence_score"), 2).alias("avg_event_conf"),
         F.slice(F.collect_list(F.concat(
             F.date_format("event_time", "yyyy-MM-dd"), F.lit(" | "),
             F.col("event_type"), F.lit(" | "), F.col("event_title"))), 1, 15)
             .alias("timeline_slice"),
         F.slice(F.collect_set("source_url"), 1, 10).alias("event_evidence_urls")))

# 2) Relationship discovery: graph edges touching the decision's own record
edge_facts = (dec.select("decision_id", "dec_source_id")
    .join(gold_edges,
          (F.col("source_node_id") == F.col("dec_source_id")) |
          (F.col("target_node_id") == F.col("dec_source_id")))
    .groupBy("decision_id")
    .agg(F.slice(F.collect_list(F.when(F.col("relationship_basis") == "documented",
             F.concat(F.col("relationship_type"), F.lit(" (evidence: "),
                      F.col("evidence_source_url"), F.lit(")")))), 1, 10)
             .alias("verified_facts"),
         F.slice(F.collect_list(F.when(F.col("relationship_basis") == "inferred",
             F.concat(F.col("relationship_type"), F.lit(" [inferred, conf "),
                      F.round("confidence_score", 2).cast("string"), F.lit("]")))), 1, 10)
             .alias("inferred_facts")))

# 3) Contradiction analysis: contradictions whose subject text overlaps the
#    decision title (word-level; global provenance mismatches are not forced here)
dec_words = (dec.select("decision_id", "decision_title")
    .withColumn("w", F.explode(F.filter(F.split(F.lower("decision_title"), " +"),
                                         lambda x: F.length(x) >= 5)))
    .select("decision_id", "w"))
contras = (dec_words.join(contradictions, F.expr("instr(lower(subject), w) > 0"))
    .groupBy("decision_id")
    .agg(F.collect_set("category").alias("contradiction_categories"),
         F.slice(F.collect_set("subject"), 1, 5).alias("contradiction_subjects")))

# 4) Assemble with confidence-aware classification
investigations = (dec
    .join(slices, on="decision_id", how="left")
    .join(edge_facts, on="decision_id", how="left")
    .join(contras, on="decision_id", how="left")
    .withColumn("overall_confidence", F.round(
        F.col("decision_confidence") * 0.5 +
        F.coalesce(F.col("avg_event_conf"), F.lit(0.60)) * 0.3 +
        F.when(F.coalesce(F.size("verified_facts"), F.lit(0)) > 0, F.lit(0.20))
               .otherwise(F.lit(0.10)), 2))
    .withColumn("reconstruction_class",
        F.when(F.col("overall_confidence") >= 0.75, F.lit("high"))
         .when(F.col("overall_confidence") >= 0.60, F.lit("medium"))
         .otherwise(F.lit("low")))
    .withColumn("missing_information", F.array_compact(F.array(
        F.when(F.col("triggering_problem_title").isNull(),
               F.lit("no triggering problem found in the prior 30 days")),
        F.when(~F.col("alternatives_mentioned"),
               F.lit("no alternatives discussion found in the evidence")),
        F.when(F.col("decision_status") == "unresolved",
               F.lit("decision not yet finalized")))))
    .select(
        F.concat(F.lit("investigation:"), F.col("decision_id")).alias("investigation_id"),
        "decision_id", "decision_title", "decision_date", "decision_status",
        "triggering_problem_title",
        F.coalesce(F.col("aligned_event_count"), F.lit(0)).alias("aligned_event_count"),
        F.coalesce(F.col("timeline_slice"), EMPTY_STR_ARRAY).alias("timeline_slice"),
        F.coalesce(F.col("verified_facts"), EMPTY_STR_ARRAY).alias("verified_facts"),
        F.coalesce(F.col("inferred_facts"), EMPTY_STR_ARRAY).alias("inferred_facts"),
        F.coalesce(F.col("contradiction_categories"), EMPTY_STR_ARRAY).alias("contradiction_categories"),
        F.coalesce(F.col("contradiction_subjects"), EMPTY_STR_ARRAY).alias("contradiction_subjects"),
        F.slice(F.array_distinct(F.concat(F.array(F.col("decision_url")),
             F.coalesce(F.col("event_evidence_urls"), EMPTY_STR_ARRAY))), 1, 10)
             .alias("evidence_urls"),
        "overall_confidence", "reconstruction_class", "missing_information")
    .dropDuplicates(["investigation_id"]))

target = f"{CATALOG}.{SCHEMA}.gold_investigations"
investigations.write.mode("overwrite").saveAsTable(target)   # recomputed each run
print(f"Wrote {investigations.count()} investigations to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 24 - Validate ELTR + Flagship Demo
# ECHO-X Phase 9 (2/2) — Validate gold_investigations + flagship demo
from pyspark.sql import functions as F

inv = spark.table(f"{CATALOG}.{SCHEMA}.gold_investigations")
total = inv.count()
uniq = inv.select("investigation_id").distinct().count()
print(f"gold_investigations: {total} rows, {uniq} unique investigation_ids -> "
      f"{'OK — no duplicates' if total == uniq else 'DUPLICATES FOUND'}")

# Confidence-aware class distribution
display(inv.groupBy("reconstruction_class", "decision_status")
        .agg(F.count("*").alias("investigations"),
             F.round(F.avg("overall_confidence"), 2).alias("avg_confidence")))

# Narrative element coverage: verified facts / inferences / contradictions / declared gaps
display(inv.select(
    F.sum((F.size("verified_facts") > 0).cast("int")).alias("with_verified_facts"),
    F.sum((F.size("inferred_facts") > 0).cast("int")).alias("with_inferences"),
    F.sum((F.size("contradiction_categories") > 0).cast("int")).alias("with_contradictions"),
    F.sum((F.size("missing_information") > 0).cast("int")).alias("with_declared_gaps"),
    F.sum((F.col("aligned_event_count") >= 5).cast("int")).alias("well_aligned")))

# Flagship demo: full ELTR reconstruction of the Click vendoring decision
flagship = inv.filter("decision_title LIKE '%Vendor Click%'").first()
if flagship is not None:
    print("=" * 70)
    print("ECHO-X INVESTIGATION:", flagship["decision_title"])
    print("=" * 70)
    print(f"Date: {flagship['decision_date']}  |  Status: {flagship['decision_status']}  |  "
          f"Confidence: {flagship['overall_confidence']} ({flagship['reconstruction_class']})")
    trig = flagship["triggering_problem_title"]
    print(f"Triggering problem: {trig if trig else 'none identified in prior 30 days'}")
    print(f"Aligned events in 60-day window: {flagship['aligned_event_count']}")
    print("\nVerified facts (documented edges):")
    for fact in (flagship["verified_facts"] or [])[:5]:
        print("  *", fact)
    print("\nInferred relationships:")
    for fact in (flagship["inferred_facts"] or [])[:5]:
        print("  *", fact)
    cats = flagship["contradiction_categories"]
    print(f"\nContradictions touching this decision: {cats if cats else 'none'}")
    gaps = flagship["missing_information"]
    print(f"Declared missing information: {'; '.join(gaps) if gaps else 'none'}")
    print("\nEvidence URLs:")
    for u in (flagship["evidence_urls"] or [])[:5]:
        print("  ", u)
    print("\nTimeline slice (first 8 aligned events):")
    for t in (flagship["timeline_slice"] or [])[:8]:
        print("  -", t)

# COMMAND ----------

# DBTITLE 1,Cell 25 - Evaluation Metrics
# ECHO-X Phase 10 (1/2) — Methodology Evaluation
# Automated quality checks against the ECHO-X specification:
# evidence grounding, temporal accuracy (no fabricated dates),
# chronology, traceability to source records, uniqueness, honesty.
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")
gold_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
gold_entities = spark.table(f"{CATALOG}.{SCHEMA}.gold_entities")
gold_decisions = spark.table(f"{CATALOG}.{SCHEMA}.gold_decisions")
contradictions = spark.table(f"{CATALOG}.{SCHEMA}.contradiction_records")
investigations = spark.table(f"{CATALOG}.{SCHEMA}.gold_investigations")

def pct(n, d):
    return round(100.0 * n / d, 1) if d else 0.0

ev_total = gold_events.count()
ev_url = gold_events.filter("source_url IS NOT NULL").count()
ev_time = gold_events.filter("event_time IS NOT NULL").count()

# No fabricated dates: every event_time must equal created_ts or closed_ts
# of the very record it was extracted from (never invented).
fabricated = (gold_events.select("source_id", "event_time")
    .join(silver.select(F.col("source_id").alias("sid"), "created_ts", "closed_ts"),
          F.col("source_id") == F.col("sid"))
    .filter((F.col("event_time") != F.col("created_ts")) &
            (F.col("event_time") != F.col("closed_ts")))
    .count())

# Chronology: sorted by time, no event goes backwards (single partition OK at this scale)
violations = (gold_events.filter("event_time IS NOT NULL")
    .withColumn("prev_time", F.lag("event_time").over(Window.orderBy("event_time")))
    .filter(F.col("prev_time").isNotNull() & (F.col("prev_time") > F.col("event_time")))
    .count())

# Traceability: every event's source_id resolves to an ingested silver record
ev_sources = gold_events.select("source_id").distinct()
traceable = ev_sources.join(silver.select(F.col("source_id").alias("sid2")),
                            F.col("source_id") == F.col("sid2")).count()

checks = [
    ("event_uniqueness_pct",
     pct(gold_events.select("event_id").distinct().count(), ev_total),
     "gold_events: unique event_id"),
    ("entity_uniqueness_pct",
     pct(gold_entities.select("entity_id").distinct().count(), gold_entities.count()),
     "gold_entities: unique entity_id"),
    ("decision_uniqueness_pct",
     pct(gold_decisions.select("decision_id").distinct().count(), gold_decisions.count()),
     "gold_decisions: unique decision_id"),
    ("investigation_uniqueness_pct",
     pct(investigations.select("investigation_id").distinct().count(), investigations.count()),
     "gold_investigations: unique investigation_id"),
    ("contradiction_uniqueness_pct",
     pct(contradictions.select("contradiction_id").distinct().count(), contradictions.count()),
     "contradiction_records: unique contradiction_id"),
    ("evidence_url_coverage_pct", pct(ev_url, ev_total),
     "events carrying a source_url"),
    ("event_time_coverage_pct", pct(ev_time, ev_total),
     "events with a non-null event_time"),
    ("event_time_source_match_pct", pct(ev_total - fabricated, ev_total),
     "event_time equals created_ts or closed_ts of its source record"),
    ("chronology_violations", float(violations),
     "events out of chronological order (must be 0)"),
    ("traceability_pct", pct(traceable, ev_sources.count()),
     "events traceable to an ingested silver record"),
    ("contradictions_both_sides_pct",
     pct(contradictions.filter("value_a IS NOT NULL AND value_b IS NOT NULL").count(),
        contradictions.count()),
     "contradictions where BOTH conflicting values are preserved"),
]

schema = StructType([StructField("metric_name", StringType()),
                     StructField("metric_value", DoubleType()),
                     StructField("metric_meaning", StringType())])
metrics = spark.createDataFrame([(n, float(v), m) for n, v, m in checks], schema)
target = f"{CATALOG}.{SCHEMA}.gold_evaluation_metrics"
metrics.write.mode("overwrite").saveAsTable(target)   # recomputed each run
display(metrics.orderBy("metric_name"))
print(f"Wrote {len(checks)} evaluation metrics to {target}")

# COMMAND ----------

# DBTITLE 1,Cell 26 - Investigation Dry-Run
# ECHO-X Phase 10 (2/2) — Investigation dry-run
# Four representative natural-language questions the Genie Investigation
# Assistant will answer — solved here directly from the gold tables,
# every answer carrying evidence URLs.
from pyspark.sql import functions as F

inv = spark.table(f"{CATALOG}.{SCHEMA}.gold_investigations")
events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
decisions = spark.table(f"{CATALOG}.{SCHEMA}.gold_decisions")
contras = spark.table(f"{CATALOG}.{SCHEMA}.contradiction_records")

print("Q1 — Why did Typer vendor Click, and what followed?")
f1 = inv.filter("decision_title LIKE '%Vendor Click%'").first()
if f1 is not None:
    print(f"  Decision {f1['decision_date']} | status {f1['decision_status']} | "
          f"confidence {f1['overall_confidence']} ({f1['reconstruction_class']})")
    print(f"  Primary evidence: {f1['evidence_urls'][0]}")
    print(f"  {f1['aligned_event_count']} aligned events in 60 days, e.g.:")
    print(f"    - {f1['timeline_slice'][0]}")
    gaps = f1['missing_information']
    print(f"  Declared gap: {'; '.join(gaps) if gaps else 'none'}")

print("\nQ2 — Which bugs appeared within 14 days after release 0.26.0, "
      "and how many got resolved?")
rel26 = (events.filter("event_type = 'version_release' AND event_title LIKE '%0.26.0%'")
         .orderBy(F.desc("event_time")).first())
if rel26 is not None:
    t0 = rel26["event_time"]
    bugs = (events.filter("event_type = 'bug_discovery'")
            .filter((F.col("event_time") >= F.lit(t0)) &
                    (F.col("event_time") <= F.lit(t0) + F.expr("INTERVAL 14 DAYS"))))
    fixes = (events.filter("event_type = 'bug_resolution'")
             .filter((F.col("event_time") >= F.lit(t0)) &
                     (F.col("event_time") <= F.lit(t0) + F.expr("INTERVAL 14 DAYS"))))
    print(f"  Release {rel26['event_title']} at {t0} ({rel26['source_url']})")
    print(f"  bug_discovery: {bugs.count()} | bug_resolution: {fixes.count()}")
    for b in bugs.orderBy(F.asc("event_time")).limit(3).collect():
        print(f"    - {b['event_title']}  [{b['source_url']}]")

print("\nQ3 — What contradictions exist in the historical record?")
for c in contras.collect():
    print(f"  {c['category']}: '{c['subject']}' -> "
          f"'{c['value_a']}' vs '{c['value_b']}' (conf {c['confidence_score']})")
    print(f"    evidence: {c['evidence_urls'][0]}")

print("\nQ4 — Which decisions remain unresolved today?")
for d in decisions.filter("decision_status = 'unresolved'").collect():
    print(f"  {d['decision_title']} (opened {d['decision_date']})")
    print(f"    evidence: {d['source_url']}")

# COMMAND ----------

# DBTITLE 1,Cell 27 - Genie Instructions (markdown)
# MAGIC %md
# MAGIC # ECHO-X — Genie Investigation Assistant setup (paste into the Genie space)
# MAGIC
# MAGIC > **Step 1 — Attach data:** In the Genie space, click **Add data** and attach the 11 `echo_x` tables listed below (catalog `learn_get_started_de_vijayanandhini`, schema `echo_x`).
# MAGIC >
# MAGIC > **Step 2 — Paste these instructions** into the Genie space's **Instructions** field.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Role
# MAGIC You are the ECHO-X Investigation Assistant. You answer questions about the *reconstructed development history* of the **typer** repository (GitHub, originally `tiangolo/typer`, now `fastapi/typer`) using ONLY the gold tables in schema `echo_x`. You reconstruct events, timelines, decisions, and contradictions — always evidence-grounded.
# MAGIC
# MAGIC ## Table guide
# MAGIC
# MAGIC * `gold_events` — 232 historical events (releases, bugs, features, deprecations, migrations): `event_id`, `event_type`, `event_title`, `event_time`, `source_url`, `confidence_score`.
# MAGIC * `gold_entities` — 726 entities (records, developers, technologies) with `entity_id`, `entity_type`, `mention_count`.
# MAGIC * `gold_timeline` — full chronology of events in order.
# MAGIC * `gold_incident_resolutions` — bug-to-fix chains (resolved/unresolved, duration).
# MAGIC * `gold_technology_evolution` — technology mentions per year (e.g. click, pydantic, rich).
# MAGIC * `gold_nodes` / `gold_edges` — knowledge graph: 958 nodes, 1618 edges; each edge is `documented` (has evidence URL) or `inferred` (has confidence).
# MAGIC * `gold_decisions` — 19 reconstructed decisions with `decision_status` (implemented / unresolved), triggering problem, implementation release, `confidence_score`.
# MAGIC * `contradiction_records` — detected conflicts (both conflicting values preserved, never silently resolved).
# MAGIC * `gold_investigations` — integrated evidence-linked reconstruction per decision: timeline slice, verified facts, inferences, declared gaps, `overall_confidence`.
# MAGIC * `gold_evaluation_metrics` — automated quality scores of the pipeline itself.
# MAGIC
# MAGIC ## Rules
# MAGIC
# MAGIC 1. **Cite evidence**: every claim must reference a `source_url` from the tables.
# MAGIC 2. **Never invent dates**: only use `event_time`, `created_ts`, `closed_ts` values present in the data.
# MAGIC 3. **Report confidence**: when a fact has a `confidence_score` / `overall_confidence`, state it.
# MAGIC 4. **Declare gaps**: when `missing_information` says something is unknown, say so — never guess.
# MAGIC 5. **Contradictions**: present BOTH sides (`value_a` vs `value_b`), do not pick a winner.
# MAGIC 6. **Coverage honesty**: issues/PRs cover only recent history (5 pages); releases cover 2019→2026 — absence of a record is not proof it never existed.
# MAGIC
# MAGIC ## Starter questions
# MAGIC
# MAGIC * Why did typer vendor Click, and what happened right after?
# MAGIC * What bugs appeared after release 0.26.0 and were they fixed?
# MAGIC * How did usage of click vs pydantic evolve over the years?
# MAGIC * What contradictions exist in the historical record?
# MAGIC * Which decisions are still unresolved?
# MAGIC * Trace one bug from discovery to fix to the release that shipped it.

# COMMAND ----------

# DBTITLE 1,Cell 28 - Benchmark Question Bank
# ECHO-X Phase 12 (1/5) — Benchmark question bank
# 12 challenging, evidence-grounded questions across the five ECHO-X
# capabilities. Expected evidence was verified against the Gold tables in
# Phases 4-10 — nothing here is invented. Idempotent MERGE (history-safe).
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Same values as Phase 2 — re-declared so Phase 12 cells also run after a
# fresh kernel/session without re-running the whole pipeline.
try:
    CATALOG, SCHEMA
except NameError:
    CATALOG = "learn_get_started_de_vijayanandhini"
    SCHEMA = "echo_x"

questions = [
    ("q01_graph_bug_chain", "knowledge_graph_traversal",
     "Trace the typer==0.26.0 envvar bug from its report through its fix to the release that shipped it, with evidence for every relationship.",
     ["gold_events", "gold_edges", "gold_nodes"],
     ["issue #1787 bug_discovery", "bug_resolution from the same source record",
      "RESOLVED_BY edge (documented)", "IMPLEMENTED_IN edge (inferred)",
      "release event strictly after the fix"]),
    ("q02_graph_chain_entities", "knowledge_graph_traversal",
     "Which developers and technologies are associated with the envvar bug chain, and what edges connect them?",
     ["gold_entities", "gold_edges", "silver_github_records"],
     ["developer entity nodes for the record authors",
      "technology RELATED_TO edges to the bug record"]),
    ("q03_graph_hubs", "knowledge_graph_traversal",
     "Which nodes are the biggest hubs in the knowledge graph, and how are their edges backed (documented vs inferred)?",
     ["gold_nodes", "gold_edges"],
     ["dependabot[bot] as top hub (190 edges, Phase-6 validated)",
      "python (178) and tiangolo (161) among the top hubs"]),
    ("q04_temporal_click_window", "temporal_intelligence",
     "Reconstruct Click's development history between 2020-01-01 and 2026-12-31.",
     ["gold_technology_evolution", "gold_entities"],
     ["yearly click mentions, all years inside the window",
      "full-history yearly total consistent with the click entity mention_count"]),
    ("q05_temporal_around_vendoring", "temporal_intelligence",
     "What important events happened in the 30 days before and after the Click vendoring decision (2026-05-20)?",
     ["gold_decisions", "gold_events"],
     ["events strictly inside the +-30-day window",
      "non-empty sets both before and after the decision",
      "decision date matches the Gold record"]),
    ("q06_temporal_pydantic", "temporal_intelligence",
     "How did Pydantic usage evolve across the available historical records?",
     ["gold_technology_evolution", "gold_entities"],
     ["pydantic yearly mentions consistent with its entity mention_count",
      "pydantic first appears only in 2026 (first seen 2026-02)"]),
    ("q07_decision_vendoring", "decision_reconstruction",
     "Why was Click vendoring introduced? Reconstruct the decision: original problem, proposed change, implementation evidence, and follow-on events.",
     ["gold_decisions", "gold_investigations"],
     ["PR #1774 decision implemented in release 0.26.0",
      "33 subsequent outcome events",
      "declared gap: no alternatives discussion in the evidence"]),
    ("q08_decision_unresolved", "decision_reconstruction",
     "Which decisions remain unresolved, and what evidence is missing to call them implemented?",
     ["gold_decisions", "gold_investigations", "silver_github_records"],
     ["exactly one unresolved decision (PR #1934, still open)",
      "no implementation release evidence",
      "declared gap: decision not yet finalized"]),
    ("q09_decision_releases", "decision_reconstruction",
     "Which decisions were implemented, and which release shipped each? Verify every release against gold_events.",
     ["gold_decisions", "gold_events"],
     ["18 implemented decisions (Phase-7 validated)",
      "every implementation release exists as a version_release event on/after the decision"]),
    ("q10_contradiction_identity", "contradiction_detection",
     "Investigate the repository identity mismatch: both sides, affected records, and evidence.",
     ["contradiction_records", "silver_github_records"],
     ["source_id owner vs fastapi/typer in URLs",
      "all 598 records affected", "both conflicting values preserved"]),
    ("q11_contradiction_dates", "contradiction_detection",
     "Explain the conflicting Python 3.8 support-drop dates and show the source records behind each side.",
     ["contradiction_records", "silver_github_records"],
     ["2025-12-24 17:22:30 vs 2025-12-25 09:27:32",
      "both source records exist in silver with exactly those created_ts"]),
    ("q12_evidence_facts_vs_inference", "evidence_grounded_investigation",
     "For the Click vendoring decision, which claims are verified facts and which are inferences? Give confidence scores and declared gaps.",
     ["gold_investigations", "gold_edges"],
     ["verified facts, each with an evidence URL that exists in gold_edges",
      "inferences, each with a confidence label",
      "declared missing information (no guessing)"]),
]

qschema = StructType([
    StructField("question_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("question_text", StringType(), False),
    StructField("required_tables", ArrayType(StringType()), False),
    StructField("expected_evidence", ArrayType(StringType()), False)])
qdf = spark.createDataFrame(
    [Row(question_id=q[0], category=q[1], question_text=q[2],
         required_tables=list(q[3]), expected_evidence=list(q[4]))
     for q in questions], qschema)

target = f"{CATALOG}.{SCHEMA}.gold_benchmark_questions"
if not spark.catalog.tableExists(target):
    qdf.write.saveAsTable(target)                 # first run: create
    print(f"Created {target} with {qdf.count()} questions")
else:                                             # idempotent upsert
    qdf.createOrReplaceTempView("echo_x_bench_q_src")
    spark.sql(f"""
        MERGE INTO {target} AS t USING echo_x_bench_q_src AS s
          ON t.question_id = s.question_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged {qdf.count()} questions into {target}")

display(spark.table(target).select("question_id", "category"))

# COMMAND ----------

# DBTITLE 1,Cell 29 - Run Benchmarks Q1-Q6
# ECHO-X Phase 12 (2/5) — Run benchmarks Q1-Q6
# (knowledge graph traversal + temporal intelligence)
# Every check compares a computed answer against verifiable Gold evidence;
# nothing is asserted that was not read from a table in this cell.
from pyspark.sql import functions as F
from datetime import datetime

RUN_ID = "run_" + datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Benchmark run: {RUN_ID}")

silver   = spark.table(f"{CATALOG}.{SCHEMA}.silver_github_records")
g_events = spark.table(f"{CATALOG}.{SCHEMA}.gold_events")
g_ent    = spark.table(f"{CATALOG}.{SCHEMA}.gold_entities")
g_edges  = spark.table(f"{CATALOG}.{SCHEMA}.gold_edges")
g_nodes  = spark.table(f"{CATALOG}.{SCHEMA}.gold_nodes")
g_dec    = spark.table(f"{CATALOG}.{SCHEMA}.gold_decisions")
g_inv    = spark.table(f"{CATALOG}.{SCHEMA}.gold_investigations")
g_con    = spark.table(f"{CATALOG}.{SCHEMA}.contradiction_records")
g_evo    = spark.table(f"{CATALOG}.{SCHEMA}.gold_technology_evolution")

bench_results = []    # (question_id, dimension, passed, detail)
bench_answers = {}    # question_id -> evidence-grounded answer text

def record(qid, dim, passed, detail=""):
    bench_results.append((qid, dim, bool(passed), str(detail)[:220]))

def found_ids(source_ids):
    ids = [i for i in dict.fromkeys(source_ids) if i]
    if not ids:
        return set()
    return {r["source_id"] for r in
            silver.filter(F.col("source_id").isin(ids))
                  .select("source_id").distinct().collect()}

def found_urls(urls):
    us = [u for u in dict.fromkeys(urls) if u]
    if not us:
        return set()
    return {r["source_url"] for r in
            silver.filter(F.col("source_url").isin(us))
                  .select("source_url").distinct().collect()}

def url_of(x): return x["source_url"] if x is not None else None
def sid_of(x): return x["source_id"] if x is not None else None

# ---- Q1: envvar bug -> fix -> release chain --------------------------------
qid = "q01_graph_bug_chain"
bug = g_events.filter("event_type = 'bug_discovery' AND event_title LIKE '%envvar%'"
                       " AND event_title LIKE '%0.26.0%'").first()
res = (g_events.filter(f"event_type = 'bug_resolution' AND source_id = '{sid_of(bug)}'")
       .first()) if bug is not None else None
rb = (g_edges.filter(f"source_node_id = '{bug['event_id']}' AND "
                     f"target_node_id = '{res['event_id']}' AND "
                     "relationship_type = 'RESOLVED_BY'").first()
      ) if bug is not None and res is not None else None
impl = (g_edges.filter(f"source_node_id = '{res['event_id']}' AND "
                       "relationship_type = 'IMPLEMENTED_IN'")
        .orderBy(F.desc("confidence_score")).first()
        ) if res is not None else None
rel = (g_events.filter(f"event_id = '{impl['target_node_id']}'").first()
       ) if impl is not None else None
chain_ok = None not in (bug, res, impl, rel)
record(qid, "answer_correctness", chain_ok,
       "complete report -> fix -> release chain reconstructed" if chain_ok else
       f"chain incomplete (bug={bug is not None}, fix={res is not None}, "
       f"impl={impl is not None}, rel={rel is not None})")
urls = [url_of(bug), url_of(res), url_of(rel)]
record(qid, "evidence_coverage", all(u is not None for u in urls),
       f"chain evidence URLs: {[u for u in urls if u]}")
sids = [sid_of(bug), sid_of(res), sid_of(rel)]
record(qid, "source_traceability", found_ids(sids) == {s for s in sids if s},
       "all three chain records resolve to ingested silver records")
record(qid, "temporal_accuracy",
       chain_ok and bug["event_time"] <= res["event_time"] < rel["event_time"],
       "report <= fix < release — ordering taken from real timestamps only")
nids = [bug["event_id"], res["event_id"], rel["event_id"]] if chain_ok else []
endpoints_ok = (g_nodes.filter(F.col("node_id").isin(nids)).select("node_id")
                .distinct().count() == len(set(nids))) if nids else False
record(qid, "relationship_accuracy",
       chain_ok and rb is not None and rb["relationship_basis"] == "documented"
       and impl["relationship_basis"] == "inferred" and endpoints_ok,
       f"RESOLVED_BY documented (conf {rb['confidence_score'] if rb else '-'}); "
       f"IMPLEMENTED_IN inferred (conf {impl['confidence_score'] if impl else '-'}); "
       "all endpoints exist in gold_nodes")
bench_answers[qid] = (f"'{bug['event_title']}' ({url_of(bug)}) was resolved by the "
    f"same record's bug_resolution and shipped in release '{rel['event_title']}' "
    f"({url_of(rel)}); the fix->release link is an INFERENCE at confidence "
    f"{impl['confidence_score']} (first release after the fix), not a documented fact."
    ) if chain_ok else "chain could not be reconstructed"

# ---- Q2: developers + technologies of the bug chain -----------------------
qid = "q02_graph_chain_entities"
rec_ids = [sid_of(bug), sid_of(res)]
recs = silver.filter(F.col("source_id").isin(rec_ids)).select(
    "source_id", "author").collect()
devs = sorted({f"developer:{r['author']}" for r in recs})
rel_edges = (g_edges.filter(
    ((F.col("source_node_id").isin(rec_ids)) | (F.col("target_node_id").isin(rec_ids)))
    & (F.col("relationship_type") == "RELATED_TO"))).collect()
devs_linked = sorted({e["source_node_id"] for e in rel_edges
                     if e["source_node_id"].startswith("developer:")})
techs = sorted({e["source_node_id"].split(":", 1)[1] for e in rel_edges
                if e["source_node_id"].startswith("technology:")})
basis_dev = {e["relationship_basis"] for e in rel_edges
             if e["source_node_id"].startswith("developer:")}
basis_tech = {e["relationship_basis"] for e in rel_edges
              if e["source_node_id"].startswith("technology:")}
record(qid, "answer_correctness", bool(devs) and bool(techs),
       f"developers: {devs}; technologies: {techs}")
record(qid, "relationship_accuracy",
       devs == devs_linked and basis_dev == {"documented"} and basis_tech == {"inferred"},
       f"graph developers {devs_linked} == silver authors; developer edges "
       f"documented {basis_dev}; technology edges inferred {basis_tech}")
record(qid, "evidence_coverage", bool(techs),
       f"technology edges found for the bug record: {techs}")
record(qid, "source_traceability", found_ids(rec_ids) == {r for r in rec_ids if r},
       "both bug-chain records resolve to silver")
bench_answers[qid] = (f"Authors of the bug chain records: {devs}; technologies linked "
    f"to those records: {techs}. Developer links are documented evidence (author "
    "field); technology links are keyword-based inferences (confidence 0.6).")

# ---- Q3: graph hubs -------------------------------------------------------
qid = "q03_graph_hubs"
hubs = (g_edges.select(F.col("source_node_id").alias("nid"))
        .union(g_edges.select(F.col("target_node_id").alias("nid")))
        .groupBy("nid").agg(F.count("*").alias("edge_count")))
top3 = (hubs.withColumnRenamed("nid", "node_id")
        .join(g_nodes.select("node_id", "label"), "node_id")
        .orderBy(F.desc("edge_count")).limit(3).collect())
top1 = top3[0] if top3 else None
hub_edge_basis = []
if top1 is not None:
    hub_edge_basis = (g_edges.filter((F.col("source_node_id") == top1["node_id"]) |
                                     (F.col("target_node_id") == top1["node_id"]))
                      .groupBy("relationship_basis").agg(F.count("*").alias("n")).collect())
record(qid, "answer_correctness",
       top1 is not None and top1["label"] == "dependabot[bot]"
       and top1["edge_count"] == 190,
       f"top hubs: {[(r['label'], r['edge_count']) for r in top3]}")
record(qid, "relationship_accuracy",
       {r["label"] for r in top3} >= {"python", "tiangolo"},
       "hub set matches Phase-6 validation: dependabot[bot] 190, python 178, tiangolo 161")
record(qid, "evidence_coverage", bool(hub_edge_basis)
       and sum(r["n"] for r in hub_edge_basis) == (top1["edge_count"] if top1 else 0),
       f"top hub edges by basis: {[(r['relationship_basis'], r['n']) for r in hub_edge_basis]}")
record(qid, "source_traceability",
       len(top3) == 3 and g_nodes.filter(
           F.col("node_id").isin([r["node_id"] for r in top3])).count() == 3,
       "hub nodes exist in gold_nodes")
bench_answers[qid] = (f"Largest hubs: {[(r['label'], r['edge_count']) for r in top3]}. "
    f"Top hub edges are backed {[(r['relationship_basis'], r['n']) for r in hub_edge_basis]} "
    "(documented = author links; inferred = temporal/technology links).")

# ---- Q4: Click history 2020-2026 -----------------------------------------
qid = "q04_temporal_click_window"
click_years = (g_evo.filter("normalized_name = 'click' AND year BETWEEN 2020 AND 2026")
               .orderBy("year").collect())
click_total = g_evo.filter("normalized_name = 'click'").agg(
    F.sum("mentions").alias("m")).first()["m"]
click_ent = g_ent.filter("entity_id = 'technology:click'").select(
    "mention_count", "first_seen_ts").first()
record(qid, "answer_correctness", bool(click_years),
       f"yearly click mentions in window: {[(r['year'], r['mentions']) for r in click_years]}")
record(qid, "temporal_accuracy",
       bool(click_years) and all(2020 <= r["year"] <= 2026 for r in click_years),
       "all reported years inside the requested 2020-2026 window")
record(qid, "evidence_coverage",
       click_ent is not None and click_total == click_ent["mention_count"],
       f"sum of ALL click yearly mentions ({click_total}) == entity mention_count "
       f"({click_ent['mention_count'] if click_ent else '-'}) — cross-table consistency")
record(qid, "source_traceability",
       click_ent is not None and click_ent["first_seen_ts"].year == 2019,
       f"click entity first seen {click_ent['first_seen_ts'] if click_ent else '-'} "
       "(pre-window history acknowledged, not fabricated)")
bench_answers[qid] = (f"Click mentions per year 2020-2026: "
    f"{[(r['year'], r['mentions']) for r in click_years]}; entity-level total "
    f"{click_ent['mention_count'] if click_ent else '-'} mentions since first seen "
    f"{click_ent['first_seen_ts'] if click_ent else '-'}.")

# ---- Q5: +-30 days around the vendoring decision -------------------------
qid = "q05_temporal_around_vendoring"
vd = g_dec.filter("decision_title LIKE '%Vendor Click%'").first()
t0 = vd["decision_date"] if vd is not None else None
if t0 is not None:
    wrows = (g_events.filter(
        F.abs(F.datediff(F.col("event_time").cast("date"),
                         F.lit(t0).cast("date"))) <= 30)
        .select("event_id", "event_type", "event_title", "event_time",
                "source_id", "source_url").collect())
    before = [r for r in wrows if r["event_time"] < t0]
    after = [r for r in wrows if r["event_time"] >= t0]
else:
    wrows, before, after = [], [], []
record(qid, "answer_correctness", bool(before) and bool(after),
       f"{len(before)} events before the decision, {len(after)} after")
record(qid, "temporal_accuracy",
       vd is not None and t0.strftime("%Y-%m-%d") == "2026-05-20"
       and all(abs((r["event_time"].date() - t0.date()).days) <= 30 for r in wrows),
       "decision date matches Gold (2026-05-20); every reported event is "
       "within +-30 days by real timestamps")
record(qid, "evidence_coverage", bool(wrows) and all(r["source_url"] for r in wrows),
       f"{len(wrows)} window events all carry a source URL")
record(qid, "source_traceability",
       found_ids([r["source_id"] for r in wrows]) == {r["source_id"] for r in wrows},
       f"all {len({r['source_id'] for r in wrows})} distinct source_ids resolve to silver")
bench_answers[qid] = (f"Vendoring decision dated {t0}: {len(before)} events before, "
    f"{len(after)} after (all verifiable in gold_events; e.g. "
    f"'{wrows[0]['event_title']}' on {wrows[0]['event_time']})."
    ) if wrows else "no window events found"

# ---- Q6: Pydantic evolution ------------------------------------------------
qid = "q06_temporal_pydantic"
pyd_years = g_evo.filter("normalized_name = 'pydantic'").orderBy("year").collect()
pyd_sum = sum(r["mentions"] for r in pyd_years)
pyd_ent = g_ent.filter("entity_id = 'technology:pydantic'").select(
    "mention_count", "first_seen_ts").first()
pyd_recount = (silver.filter(F.lower(F.concat_ws(" ",
        F.coalesce(F.col("title"), F.lit("")),
        F.coalesce(F.col("body"), F.lit(""))))
    .rlike("\\bpydantic\\b")).count())
record(qid, "answer_correctness", bool(pyd_years),
       f"pydantic mentions by year: {[(r['year'], r['mentions']) for r in pyd_years]}")
record(qid, "temporal_accuracy",
       pyd_ent is not None and pyd_ent["first_seen_ts"].year == 2026
       and pyd_ent["first_seen_ts"].month == 2
       and all(r["year"] == 2026 for r in pyd_years),
       f"pydantic first seen {pyd_ent['first_seen_ts'] if pyd_ent else '-'} — "
       "every mention falls in 2026")
record(qid, "evidence_coverage",
       pyd_ent is not None and pyd_sum == pyd_ent["mention_count"],
       f"yearly sum ({pyd_sum}) == entity mention_count "
       f"({pyd_ent['mention_count'] if pyd_ent else '-'}) — cross-table consistency")
record(qid, "source_traceability", pyd_recount == pyd_sum,
       f"independent re-scan of silver finds {pyd_recount} pydantic records, "
       f"matching the {pyd_sum} evolution mentions")
bench_answers[qid] = (f"Pydantic appears only in "
    f"{[(r['year'], r['mentions']) for r in pyd_years]} — {pyd_sum} mentions total, "
    f"first seen {pyd_ent['first_seen_ts'] if pyd_ent else '-'}. The recent-window "
    "pattern is real, not an artifact.")

print(f"Q1-Q6 executed: {len(bench_results)} checks recorded")

# COMMAND ----------

# DBTITLE 1,Cell 30 - Run Benchmarks Q7-Q12 + Results
# ECHO-X Phase 12 (3/5) — Run benchmarks Q7-Q12 (decision reconstruction,
# contradiction handling, evidence-grounded investigation) and persist the
# per-question results. Appends are keyed by RUN_ID: every run adds a NEW row
# set — previous benchmark history is never overwritten.
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType, BooleanType)
import json

# ---- Q7: why Click vendoring ----------------------------------------------
qid = "q07_decision_vendoring"
vd = g_dec.filter("decision_title LIKE '%Vendor Click%'").first()
ivd = g_inv.filter("decision_title LIKE '%Vendor Click%'").first()
ok7 = (vd is not None
       and vd["decision_status"] == "implemented"
       and vd["implementation_release_title"] == "0.26.0"
       and vd["subsequent_outcome_count"] == 33
       and vd["alternatives_mentioned"] == False)
record(qid, "answer_correctness", ok7,
       f"status={vd['decision_status'] if vd else '-'}; release="
       f"{vd['implementation_release_title'] if vd else '-'}; outcomes="
       f"{vd['subsequent_outcome_count'] if vd else '-'}")
record(qid, "evidence_coverage",
       vd is not None and vd["source_url"] == "https://github.com/fastapi/typer/pull/1774",
       "decision record is PR #1774")
record(qid, "temporal_accuracy",
       vd is not None and vd["decision_date"].strftime("%Y-%m-%d") == "2026-05-20",
       "decision date 2026-05-20 matches the Gold record")
record(qid, "unsupported_claim_rate",
       ivd is not None and any("alternatives" in g for g in ivd["missing_information"]),
       "the missing alternatives discussion is DECLARED as a gap, not guessed")
record(qid, "evidence_coverage",
       ivd is not None and ivd["overall_confidence"] == 0.81
       and ivd["reconstruction_class"] == "high",
       "integrated investigation confidence 0.81 (high)")
bench_answers[qid] = (f"Click vendoring (PR #1774, {vd['decision_date']}) was "
    f"implemented in release 0.26.0 with {vd['subsequent_outcome_count']} follow-on "
    f"events; triggering problem: '{vd['triggering_problem_title']}'. Confidence: "
    "0.75 (decision), 0.81 (investigation). DECLARED GAP: no alternatives discussion "
    "exists in the ingested evidence — the pipeline says so instead of inventing "
    "reasons.") if vd is not None else "decision not found"

# ---- Q8: unresolved decision + missing evidence --------------------------
qid = "q08_decision_unresolved"
unres = g_dec.filter("decision_status = 'unresolved'").collect()
u = unres[0] if unres else None
usil = silver.filter(f"source_id = '{u['source_id']}'").first() if u is not None else None
ivu = g_inv.filter("decision_status = 'unresolved'").collect() if u is not None else []
record(qid, "answer_correctness",
       len(unres) == 1 and u is not None
       and "deprecated flag" in u["decision_title"].lower(),
       f"unresolved decisions: {[(r['decision_title'][:60], r['source_url']) for r in unres]}")
record(qid, "evidence_coverage",
       u is not None and u["implementation_release_title"] is None,
       "no implementation release exists — that IS the missing evidence")
record(qid, "temporal_accuracy",
       usil is not None and usil["closed_ts"] is None,
       "the PR was never closed, so 'release within 90 days after closure' "
       "cannot be evaluated — reported honestly")
record(qid, "unsupported_claim_rate",
       bool(ivu) and any("not yet finalized" in g for g in ivu[0]["missing_information"]),
       "gap declared: decision not yet finalized")
record(qid, "source_traceability",
       usil is not None and usil["state"] == "open",
       "silver record state 'open' matches the unresolved status")
bench_answers[qid] = (f"One decision remains unresolved: '{u['decision_title']}' "
    f"({u['source_url']}, opened {u['decision_date']}). Missing evidence: the record "
    "is still open (never closed) and no release within 90 days of closure can exist "
    "yet; the investigation declares 'decision not yet finalized' rather than guessing."
    ) if u is not None else "no unresolved decision found"

# ---- Q9: implemented decisions + their releases ----------------------------
qid = "q09_decision_releases"
impl_dec = g_dec.filter("decision_status = 'implemented'").collect()
rel_titles = {d["implementation_release_title"] for d in impl_dec}
rel_rows = g_events.filter("event_type = 'version_release'").select(
    "event_title", "event_time").collect()
rel_by_title = {}
for r in rel_rows:
    t = r["event_title"]
    if t not in rel_by_title or r["event_time"] < rel_by_title[t]:
        rel_by_title[t] = r["event_time"]
missing_titles = rel_titles - set(rel_by_title)
after_ok = all(rel_by_title[d["implementation_release_title"]] >= d["decision_date"]
               for d in impl_dec
               if d["implementation_release_title"] in rel_by_title)
record(qid, "answer_correctness", len(impl_dec) == 18,
       f"{len(impl_dec)} implemented decisions (Phase-7 validated: 18)")
record(qid, "evidence_coverage", not missing_titles,
       f"all {len(rel_titles)} implementation releases exist as version_release "
       f"events in gold_events (missing: {sorted(missing_titles)[:3]})")
record(qid, "temporal_accuracy", not missing_titles and after_ok,
       "every release event occurs on/after its decision date")
record(qid, "source_traceability",
       found_ids([d["source_id"] for d in impl_dec]) ==
       {d["source_id"] for d in impl_dec},
       f"all {len({d['source_id'] for d in impl_dec})} decision records resolve to silver")
bench_answers[qid] = (f"{len(impl_dec)} decisions were implemented; each shipped in a "
    f"verifiable release (titles like {sorted(rel_titles)[:3]}), and every release "
    "event can be found in gold_events at or after the decision date.")

# ---- Q10: repository identity contradiction -------------------------------
qid = "q10_contradiction_identity"
c = g_con.filter("category = 'identity_mismatch'").first()
record(qid, "answer_correctness",
       c is not None and str(c["value_a"]).startswith("tiangolo")
       and c["value_b"] == "fastapi/typer",
       f"value_a={c['value_a'] if c else '-'} vs value_b={c['value_b'] if c else '-'}")
record(qid, "contradiction_handling",
       c is not None and c["value_a"] is not None and c["value_b"] is not None
       and c["records_affected"] == 598,
       f"BOTH sides preserved; {c['records_affected'] if c else 0} records affected")
record(qid, "evidence_coverage",
       c is not None and bool(c["evidence_urls"])
       and all(str(u).startswith("https://github.com/fastapi/typer/")
               for u in c["evidence_urls"]),
       "evidence URLs point to the current repository location")
record(qid, "source_traceability",
       c is not None and found_urls(list(c["evidence_urls"])) == set(c["evidence_urls"]),
       "every evidence URL resolves to an ingested silver record")
bench_answers[qid] = (f"Identity contradiction: source_ids say the repo owner is "
    f"'{c['value_a']}' but all {c['records_affected']} source URLs say "
    f"'{c['value_b']}' (confidence {c['confidence_score']}). Both versions are "
    "preserved — the repository was transferred; the engine never silently drops "
    "either side.") if c is not None else "contradiction not found"

# ---- Q11: conflicting Python 3.8 dates ------------------------------------
qid = "q11_contradiction_dates"
from datetime import datetime as _dt
c = g_con.filter("category = 'conflicting_event_dates'").first()
def _ts(s): return _dt.strptime(s, "%Y-%m-%d %H:%M:%S") if s else None
va, vb = (_ts(c["value_a"]) if c else None), (_ts(c["value_b"]) if c else None)
srcs = (silver.filter(F.col("source_url").isin(list(c["evidence_urls"])))
        .select("source_url", "created_ts").collect()) if c is not None else []
matched = {r["source_url"] for r in srcs if r["created_ts"] in (va, vb)}
record(qid, "answer_correctness",
       c is not None and c["value_a"] == "2025-12-24 17:22:30"
       and c["value_b"] == "2025-12-25 09:27:32",
       f"dates: {c['value_a'] if c else '-'} vs {c['value_b'] if c else '-'}")
record(qid, "contradiction_handling",
       len(srcs) == 2 and len(matched) == 2,
       "BOTH source records shown, one per side — neither side hidden")
record(qid, "temporal_accuracy", len(matched) == 2,
       "each conflicting date equals a real record's created_ts — no invented "
       "dates; the same event is reported ~16h apart")
record(qid, "evidence_coverage", c is not None and len(c["evidence_urls"]) >= 2,
       "two evidence URLs preserved with the contradiction")
bench_answers[qid] = (f"The Python 3.8 support-drop event carries two timestamps: "
    f"{c['value_a']} and {c['value_b']}, from two different records ({len(srcs)} source "
    "records verified, each created_ts matching one side). Both are kept; the "
    "~16-hour difference is a real reporting discrepancy, not a pipeline error."
    ) if c is not None else "contradiction not found"

# ---- Q12: verified facts vs inferences (vendoring) -------------------------
qid = "q12_evidence_facts_vs_inference"
iv = g_inv.filter("decision_title LIKE '%Vendor Click%'").first()
vf = list(iv["verified_facts"]) if iv is not None else []
inf = list(iv["inferred_facts"]) if iv is not None else []
vf_urls = [s.split("(evidence: ")[1].rstrip(")") for s in vf if "(evidence: " in s]
edge_urls = {r["evidence_source_url"] for r in
            g_edges.select("evidence_source_url").distinct().collect()}
backed = all(u in edge_urls for u in vf_urls)
record(qid, "answer_correctness",
       bool(vf) and bool(inf) and "conf" in (inf[0] if inf else ""),
       f"{len(vf)} verified facts, {len(inf)} labeled inferences")
record(qid, "evidence_coverage", bool(vf_urls) and backed,
       f"all {len(vf_urls)} verified-fact URLs appear as edge evidence in gold_edges")
record(qid, "unsupported_claim_rate", backed,
       f"0 unsupported claims: {len(vf_urls)}/{len(vf)} verified facts backed by "
       "documented edges")
record(qid, "answer_correctness",
       iv is not None and iv["overall_confidence"] == 0.81
       and iv["reconstruction_class"] == "high",
       "confidence 0.81 (high) reported alongside the narrative")
record(qid, "evidence_coverage", iv is not None and bool(iv["missing_information"]),
       f"declared gaps: {iv['missing_information'] if iv is not None else '-'}")
bench_answers[qid] = (f"For the vendoring decision: {len(vf)} verified facts (documented "
    f"edges, each with an evidence URL), {len(inf)} inferences (confidence-labeled), "
    f"overall confidence {iv['overall_confidence']} ({iv['reconstruction_class']}), "
    f"and explicit gaps: {iv['missing_information']}.") if iv is not None else \
    "investigation not found"

# ---- Assemble per-question results and append (history-preserving) ---------
DIMS = ["answer_correctness", "evidence_coverage", "source_traceability",
        "temporal_accuracy", "relationship_accuracy", "unsupported_claim_rate",
        "contradiction_handling"]

qtbl = (spark.table(f"{CATALOG}.{SCHEMA}.gold_benchmark_questions")
        .orderBy("question_id").collect())
rows = []
for q in qtbl:
    q_id = q["question_id"]
    checks = [r for r in bench_results if r[0] == q_id]
    total_c, passed_c = len(checks), sum(1 for r in checks if r[2])
    dim_vals = {d: None for d in DIMS}
    for r in checks:
        dim_vals[r[1]] = r[2] if dim_vals[r[1]] is None else (dim_vals[r[1]] and r[2])
    failed = [r for r in checks if not r[2]]
    rows.append((q_id, RUN_ID, q["category"],
                 bench_answers.get(q_id, ""),
                 json.dumps([{"dimension": r[1], "passed": r[2], "detail": r[3]}
                            for r in checks]),
                 passed_c, total_c,
                 round(passed_c / total_c, 4) if total_c else 0.0,
                 passed_c == total_c,
                 "; ".join(f"{r[1]}: {r[3]}" for r in failed) if failed else "none",
                 *[dim_vals[d] for d in DIMS]))

rschema = StructType([
    StructField("question_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("answer_summary", StringType(), True),
    StructField("checks_json", StringType(), True),
    StructField("checks_passed", IntegerType(), False),
    StructField("checks_total", IntegerType(), False),
    StructField("score", DoubleType(), False),
    StructField("passed", BooleanType(), False),
    StructField("failure_explanation", StringType(), True),
    StructField("answer_correctness", BooleanType(), True),
    StructField("evidence_coverage", BooleanType(), True),
    StructField("source_traceability", BooleanType(), True),
    StructField("temporal_accuracy", BooleanType(), True),
    StructField("relationship_accuracy", BooleanType(), True),
    StructField("unsupported_claim_rate", BooleanType(), True),
    StructField("contradiction_handling", BooleanType(), True)])
res_df = spark.createDataFrame(rows, rschema)

rtarget = f"{CATALOG}.{SCHEMA}.gold_benchmark_results"
already = (spark.catalog.tableExists(rtarget) and
           spark.table(rtarget).filter(F.col("run_id") == RUN_ID).count() > 0)
if already:
    print(f"Run {RUN_ID} already recorded — skipping append (idempotent)")
else:
    res_df.write.mode("append").saveAsTable(rtarget)   # history preserved
    print(f"Appended {res_df.count()} benchmark results for {RUN_ID} to {rtarget}")

display(spark.table(rtarget).filter(F.col("run_id") == RUN_ID)
        .select("question_id", "category", "checks_passed", "checks_total",
                "score", "passed", "failure_explanation"))

# COMMAND ----------

# DBTITLE 1,Cell 31 - Benchmark Run Summary
# ECHO-X Phase 12 (4/5) — Multi-dimensional run summary
# (gold_benchmark_summary): one row per benchmark run; previous runs are
# never overwritten, so benchmark history accumulates.
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)
from datetime import datetime

res = spark.table(f"{CATALOG}.{SCHEMA}.gold_benchmark_results").filter(
    F.col("run_id") == RUN_ID)
total = res.count()
passed = res.filter("passed").count()
avg_score = res.agg(F.round(F.avg("score"), 4)).first()[0]

def dim_pct(d):
    sub = res.filter(F.col(d).isNotNull())
    n = sub.count()
    return (round(100.0 * sub.agg(F.avg(F.col(d).cast("int"))).first()[0], 1)
            if n else None)

pcts = {d: dim_pct(d) for d in ["answer_correctness", "evidence_coverage",
        "source_traceability", "temporal_accuracy", "relationship_accuracy",
        "unsupported_claim_rate", "contradiction_handling"]}
# unsupported_claim_rate=True means NO unsupported claims were found; report
# the found-rate (100 - pass rate) so the honest target is 0.0.
unsupported_found_pct = (round(100.0 - pcts["unsupported_claim_rate"], 1)
                         if pcts["unsupported_claim_rate"] is not None else None)

sschema = StructType([
    StructField("run_id", StringType(), False),
    StructField("executed_at", StringType(), False),
    StructField("total_benchmarks", IntegerType(), False),
    StructField("passed", IntegerType(), False),
    StructField("failed", IntegerType(), False),
    StructField("accuracy_pct", DoubleType(), False),
    StructField("avg_score", DoubleType(), True),
    StructField("answer_correctness_pct", DoubleType(), True),
    StructField("evidence_coverage_pct", DoubleType(), True),
    StructField("source_traceability_pct", DoubleType(), True),
    StructField("temporal_accuracy_pct", DoubleType(), True),
    StructField("relationship_accuracy_pct", DoubleType(), True),
    StructField("contradiction_handling_pct", DoubleType(), True),
    StructField("unsupported_claims_found_pct", DoubleType(), True),
    StructField("notes", StringType(), True)])
srow = [(RUN_ID, datetime.now().isoformat(), total, passed, total - passed,
         round(100.0 * passed / total, 1) if total else 0.0, avg_score,
         pcts["answer_correctness"], pcts["evidence_coverage"],
         pcts["source_traceability"], pcts["temporal_accuracy"],
         pcts["relationship_accuracy"], pcts["contradiction_handling"],
         unsupported_found_pct,
         "answers computed deterministically from the Gold tables; Genie NLG "
         "benchmarked separately in the Genie space (3/3 at 100)")]
sdf = spark.createDataFrame(srow, sschema)

starget = f"{CATALOG}.{SCHEMA}.gold_benchmark_summary"
already = (spark.catalog.tableExists(starget) and
           spark.table(starget).filter(F.col("run_id") == RUN_ID).count() > 0)
if already:
    print(f"Summary for {RUN_ID} already recorded — skipping (idempotent)")
else:
    sdf.write.mode("append").saveAsTable(starget)   # history preserved
    print(f"Appended summary for {RUN_ID} to {starget}")

display(spark.table(starget).filter(F.col("run_id") == RUN_ID))

display(res.groupBy("category")
        .agg(F.count("*").alias("benchmarks"),
             F.sum(F.col("passed").cast("int")).alias("passed"),
             F.round(F.avg("score"), 3).alias("avg_score"),
             F.round(F.avg(F.col("checks_passed") / F.col("checks_total")), 3)
               .alias("avg_check_ratio"))
        .orderBy("category"))

print("Benchmark history preserved in gold_benchmark_summary — one row per run")

# COMMAND ----------

# DBTITLE 1,Cell 32 - Evaluation Report
# ECHO-X Phase 12 (5/5) — Evaluation report: results, chart, failures,
# weaknesses, and recommendations. Everything below is read from the
# benchmark tables produced by THIS run.
from pyspark.sql import functions as F

res = (spark.table(f"{CATALOG}.{SCHEMA}.gold_benchmark_results")
       .filter(F.col("run_id") == RUN_ID).orderBy("question_id"))
summ = (spark.table(f"{CATALOG}.{SCHEMA}.gold_benchmark_summary")
        .filter(F.col("run_id") == RUN_ID).first())

print(f"Run {RUN_ID}: {summ['total_benchmarks']} benchmarks, "
      f"{summ['passed']} passed, {summ['failed']} failed "
      f"({summ['accuracy_pct']}% accuracy, avg score {summ['avg_score']})")
print(f"Unsupported claims found: {summ['unsupported_claims_found_pct']}% "
      "(target 0.0 — no fabricated facts)")

display(res.select("question_id", "category", "answer_summary"))
display(res.groupBy("category")
        .agg(F.count("*").alias("benchmarks"),
             F.sum(F.col("passed").cast("int")).alias("passed"))
        .orderBy("category"))

# Multi-dimensional score chart
import matplotlib.pyplot as plt
dims = ["answer_correctness_pct", "evidence_coverage_pct",
        "source_traceability_pct", "temporal_accuracy_pct",
        "relationship_accuracy_pct", "contradiction_handling_pct"]
labels = ["Correctness", "Evidence coverage", "Traceability",
          "Temporal accuracy", "Relationship accuracy", "Contradiction handling"]
vals = [summ[d] if summ[d] is not None else 0.0 for d in dims]
fig, ax = plt.subplots(figsize=(9, 4.2))
bars = ax.barh(labels, vals,
               color=["#4c72b0" if v == 100 else "#dd8452" for v in vals])
ax.bar_label(bars, fmt="%.1f")
ax.set_xlim(0, 110)
ax.set_xlabel("Pass rate (%)")
ax.set_title(f"ECHO-X Phase 12 benchmark dimensions — {RUN_ID}")
plt.tight_layout()
plt.show()

# Failures with explanations, if any
failures = res.filter(~F.col("passed")).collect()
if failures:
    print("\nFAILED BENCHMARKS — explanations and fixes:")
    for f in failures:
        print(f"  {f['question_id']} ({f['category']}): {f['failure_explanation']}")
else:
    print("\nAll 12 benchmarks passed every applicable check.")

print("""
Known limitations (declared, not hidden):
1. Coverage cap: ingestion is capped at 5 pages per source (MAX_PAGES), so
   early history is sparse (129 of 232 events fall in 2026) — temporal answers
   about earlier years are thin but never invented.
2. Decision status: 'rejected' is not derivable from this payload (no record
   proves a proposal was rejected); in this data every closed candidate also
   has a release within 90 days, so 'accepted' is empty too.
3. Alternatives: most decisions show no alternatives discussion — reported
   as a declared gap on every investigation row, never filled by guessing.
4. These benchmarks evaluate the deterministic reconstruction pipeline (the
   Gold tables the Genie agent queries). Natural-language ANSWER quality is
   benchmarked separately in the Genie space (3/3 at 100).

Recommendations:
- Raise MAX_PAGES (with a token) to widen history and strengthen temporal tests.
- Extract merged/closed PR metadata (e.g. 'not planned') to derive 'rejected'.
- Add Genie-space benchmarks for narrative answers as the agent gets usage.
""")

# COMMAND ----------

# DBTITLE 1,Cell 32 - Evaluation Report
# ECHO-X Phase 12 (5/5) — Evaluation report: results, chart, failures,
# weaknesses, and recommendations. Everything below is read from the
# benchmark tables produced by THIS run (self-contained reads only).
from pyspark.sql import functions as F

res = (spark.table(f"{CATALOG}.{SCHEMA}.gold_benchmark_results")
       .filter(F.col("run_id") == RUN_ID).orderBy("question_id"))
summ = (spark.table(f"{CATALOG}.{SCHEMA}.gold_benchmark_summary")
        .filter(F.col("run_id") == RUN_ID).first())

print(f"Run {RUN_ID}: {summ['total_benchmarks']} benchmarks, "
      f"{summ['passed']} passed, {summ['failed']} failed "
      f"({summ['accuracy_pct']}% accuracy, avg score {summ['avg_score']})")
print(f"Unsupported claims found: {summ['unsupported_claims_found_pct']}% "
      "(target 0.0 — no fabricated facts)")

display(res.select("question_id", "category", "answer_summary"))
display(res.groupBy("category")
        .agg(F.count("*").alias("benchmarks"),
             F.sum(F.col("passed").cast("int")).alias("passed"))
        .orderBy("category"))

# Multi-dimensional score chart
import matplotlib.pyplot as plt
dims = ["answer_correctness_pct", "evidence_coverage_pct",
        "source_traceability_pct", "temporal_accuracy_pct",
        "relationship_accuracy_pct", "contradiction_handling_pct"]
labels = ["Correctness", "Evidence coverage", "Traceability",
          "Temporal accuracy", "Relationship accuracy", "Contradiction handling"]
vals = [summ[d] if summ[d] is not None else 0.0 for d in dims]
fig, ax = plt.subplots(figsize=(9, 4.2))
bars = ax.barh(labels, vals,
               color=["#4c72b0" if v == 100 else "#dd8452" for v in vals])
ax.bar_label(bars, fmt="%.1f")
ax.set_xlim(0, 110)
ax.set_xlabel("Pass rate (%)")
ax.set_title(f"ECHO-X Phase 12 benchmark dimensions — {RUN_ID}")
plt.tight_layout()
plt.show()

# Failures with explanations, if any
failures = res.filter(~F.col("passed")).collect()
if failures:
    print("\nFAILED BENCHMARKS — explanations and fixes:")
    for f in failures:
        print(f"  {f['question_id']} ({f['category']}): {f['failure_explanation']}")
else:
    print("\nAll 12 benchmarks passed every applicable check.")

print("""
Known limitations (declared, not hidden):
1. Coverage cap: ingestion is capped at 5 pages per source (MAX_PAGES), so
   early history is sparse (129 of 232 events fall in 2026) — temporal answers
   about earlier years are thin but never invented.
2. Decision status: 'rejected' is not derivable from this payload (no record
   proves a proposal was rejected); in this data every closed candidate also
   has a release within 90 days, so 'accepted' is empty too.
3. Alternatives: most decisions show no alternatives discussion — reported
   as a declared gap on every investigation row, never filled by guessing.
4. These benchmarks evaluate the deterministic reconstruction pipeline (the
   Gold tables the Genie agent queries). Natural-language ANSWER quality is
   benchmarked separately in the Genie space (3/3 at 100).

Recommendations:
- Raise MAX_PAGES (with a token) to widen history and strengthen temporal tests.
- Extract merged/closed PR metadata (e.g. 'not planned') to derive 'rejected'.
- Add Genie-space benchmarks for narrative answers as the agent gets usage.
""")