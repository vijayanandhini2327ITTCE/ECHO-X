# ECHO-X — Architecture Diagrams

Publication-quality Mermaid diagrams. Render in GitHub, GitLab, VS Code (Markdown Preview
Mermaid), or [mermaid.live](https://mermaid.live).

## 1. Overall ECHO-X Architecture

```mermaid
flowchart TB
    subgraph SRC["Source"]
        GH["GitHub REST API v3<br/>tiangolo/typer → fastapi/typer<br/>issues · PRs · releases"]
    end
    subgraph LAKEHOUSE["Databricks Lakehouse — Unity Catalog · echo_x"]
        B["BRONZE<br/>3 raw tables<br/>12 / 500 / 86 rows"]
        S["SILVER<br/>silver_github_records<br/>598 typed records"]
        subgraph GOLD["GOLD — 15 tables"]
            GE[gold_events · 232]
            GEN[gold_entities · 726]
            TL[gold_timeline · 232]
            CH[gold_incident_resolutions · 35]
            TE[gold_technology_evolution · 43]
            NG[gold_nodes · 958 / gold_edges · 1618]
            DE[gold_decisions · 19]
            CO[contradiction_records · 2]
            IN[gold_investigations · 19<br/>ELTR]
            EM[gold_evaluation_metrics · 11]
            BM[benchmarks 12 / 12 / 1]
        end
    end
    subgraph CONSUMERS["Consumers"]
        GENIE["Genie Agent<br/>ECHO-X Temporal Intelligence<br/>Historical Reconstruction"]
        APP["Databricks App · echo-x<br/>Streamlit · 4 modules · read-only"]
    end
    USER["Researcher / Interviewer"]

    GH -->|"HTTP + pagination + rate limits<br/>MERGE-idempotent on source_id"| B
    B -->|"clean · type · dedupe<br/>(overwrite by design — derived)"| S
    S -->|"rule extraction · real timestamps only"| GE
    S --> GEN
    GE --> TL & CH & TE
    GEN --> NG
    GE & GEN --> NG
    S & GE & NG --> DE
    S --> CO
    DE & GE & NG & CO --> IN
    IN --> EM
    IN --> BM
    GOLD --> GENIE
    GOLD -->|"parameterized read-only SQL<br/>app service principal"| APP
    GENIE -.->|"Genie API · answer + evidence SQL"| APP
    USER --> APP
    USER --> GENIE
```

## 2. Bronze–Silver–Gold Data Pipeline

```mermaid
flowchart LR
    subgraph B["Bronze — raw, append-safe"]
        BI[bronze_github_issues<br/>12]:::bronze
        BP[bronze_github_pull_requests<br/>500]:::bronze
        BR[bronze_github_releases<br/>86]:::bronze
    end
    subgraph S["Silver — typed, deduplicated"]
        SR[silver_github_records<br/>598 · 15 cols<br/>created_ts · updated_ts · closed_ts<br/>state · author · labels · payload]:::silver
    end
    subgraph G["Gold — enriched intelligence"]
        direction TB
        G1[gold_events 232<br/>10 types · real timestamps]:::gold
        G2[gold_entities 726<br/>records · developers · technologies]:::gold
        G3[temporal layer<br/>timeline 232 · chains 35 · evolution 43]:::gold
        G4[knowledge graph<br/>nodes 958 · edges 1618]:::gold
        G5[decisions 19 ·<br/>contradictions 2 ·<br/>investigations 19]:::gold
        G6[evaluation 11 ·<br/>benchmarks 12/12/1]:::gold
        G1 --> G3 --> G4 --> G5 --> G6
    end
    BI & BP & BR -->|"union + dedupe + types<br/>source_id = gh:owner/repo:type:n"| SR
    SR -->|"title regex + labels"| G1
    SR -->|"authors + tech mentions"| G2
    classDef bronze fill:#9e6a03,color:#fff
    classDef silver fill:#8b949e,color:#fff
    classDef gold fill:#58a6ff,color:#0e1117
```

## 3. ELTR Investigation Workflow

```mermaid
flowchart TB
    D["Reconstructed decision<br/>(gold_decisions · 19)"] --> TS
    TS["Timeline slicing<br/>events ±60 days (gold_timeline)"] --> VF
    VF["Verified facts<br/>documented edges only<br/>each with evidence URL"] --> CONF
    TS --> INF["Inferred facts<br/>pattern-derived · confidence-labeled"] --> CONF
    CO["Contradiction scan<br/>both sides attached<br/>(contradiction_records)"] --> CONF
    CONF["Confidence score<br/>0.5·decision + 0.3·alignment +<br/>0.2·verified bonus<br/>→ class high / medium"] --> GAP
    GAP["Declared gaps<br/>missing_information<br/>(what could NOT be reconstructed)"] --> OUT
    OUT["gold_investigations · 19<br/>avg confidence 0.80<br/>17/19 with honest gaps"]
    classDef out fill:#3fb950,color:#0e1117
    class OUT out
```

## 4. Knowledge Graph Relationship Model

```mermaid
flowchart LR
    subgraph nodes["Node types (gold_nodes · 958)"]
        EV["event nodes · 232<br/>bug · feature · release ·<br/>deprecation · migration"]
        DV["developer · 117<br/>e.g. tiangolo · dependabot&#91;bot&#93;"]
        TC["technology · 11<br/>python · click · pydantic …"]
        RC["record · 598<br/>issues · PRs · releases"]
    end
    EV -->|"RESOLVED_BY · 25<br/>documented"| EV2["bug_resolution event"]
    EV -->|"IMPLEMENTED_IN · 25<br/>inferred from timestamps"| RL["version_release"]
    EV -->|"PRECEDED_BY · 231<br/>inferred from sequence"| EV
    RL -->|"SUPERSEDED_BY · 85<br/>inferred from release order"| RL
    DV -->|"RELATED_TO · 598<br/>documented authorship"| RC
    TC -->|"RELATED_TO · 654<br/>inferred from mentions"| RC
    style EV fill:#58a6ff,color:#0e1117
    style RL fill:#39c5cf,color:#0e1117
    style DV fill:#bc8cff,color:#0e1117
    style TC fill:#3fb950,color:#0e1117
    style RC fill:#8b949e,color:#0e1117
```

**Legend:** solid labeled edges — documented (evidence URL attached); “inferred” edges —
pattern-derived, confidence-scored, and labeled as such in the app UI (green vs amber).

## 5. Application Architecture

```mermaid
flowchart LR
    U["User (SSO)<br/>workspace members"] -->|HTTPS| GW["Databricks Apps gateway<br/>echo-x.databricksapps.com"]
    GW --> APP["Streamlit app (MEDIUM compute)<br/>Dashboard · Timeline · Graph · Assistant"]
    APP -->|"WorkspaceClient()<br/>app SP OAuth — no secrets in code"| SDK["Databricks SDK"]
    SDK -->|"statement_execution<br/>parameterized SQL (STRING/NUMBER)<br/>wait 50s + poll"| WH["Serverless SQL Warehouse<br/>db585e0c39b6753f"]
    WH --> UC["Unity Catalog<br/>echo_x · 18 Delta tables<br/>GRANT SELECT only"]
    SDK -->|"Genie API<br/>start_conversation →<br/>answer + evidence SQL"| G["Genie Agent"]
    G -.->|"unavailable / no SP access"| F["Deterministic ELTR fallback<br/>same Gold tables<br/>evidence-only answers"]
    APP --> F
    classDef secure fill:#3fb950,color:#0e1117
    class UC secure
```