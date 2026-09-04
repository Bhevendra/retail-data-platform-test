# Data Engineering Project Template

> Copy this file to `docs/project-charter.md` (or `classes/class00-requirements.md` for
> the bootcamp) and fill it in **with** the stakeholders, not for them. Sections are
> numbered in the order they are usually discovered, not the order they are finished.

## How to use this template

**You will not have everything at the start.** Every section carries a *gate*:

| Gate | Meaning |
| --- | --- |
| 🟥 **Must have before implementation starts** | Without it you cannot write a single correct line: source system details, access, ownership, the business scenario, confidentiality. |
| 🟧 **Must have before Silver/Gold design** | You can land raw data without it, but you cannot model: business definitions, KPIs, refresh/latency, quality rules, target design. |
| 🟩 **Can be completed during / after build** | Consumption details, physical tuning, orchestration specifics, dictionary, lineage, runbook. Filled as decisions are made; kept in sync afterwards. |

Rules:

1. Every section has an **owner** (a person, not a team) and a **status**: `unknown`, `draft`, `agreed`, `changed on <date>`.
2. Write "unknown" rather than guessing. An explicit unknown is a task; a guess is a future incident.
3. When a section changes after implementation started, record it in the **Change log** (§22) and update the traceability table (§21).
4. Profile the data before signing off §5, §9 and §10 — real data overrules what anyone tells you.

---

## 1. Business scenario 🟥

Owner: ______ · Status: ______

* One paragraph: who has what problem today, what happens if nothing changes.
* The trigger (email, incident, regulation, new product).
* Success in one sentence, from the sponsor's mouth.
* What is explicitly **out of scope** for this version.

## 2. Stakeholders and ownership (RACI) 🟥

Owner: ______ · Status: ______

| Role | Name | Responsible / Accountable / Consulted / Informed | Decides on |
| --- | --- | --- | --- |
| Sponsor | | A | scope, priority, sign-off |
| Product / analytics lead | | R | business definitions, KPIs |
| Data engineering | | R | design, implementation |
| Source owner per system | | C | access, schema changes, extract windows |
| Data steward / governance | | C | classification, retention, quality thresholds |
| Security | | C | access model, secrets, PII handling |
| Consumers (BI, AI, finance) | | I/C | acceptance questions |

Escalation path and decision deadline for open questions.

## 3. Business questions and KPIs 🟧

Owner: ______ · Status: ______

| # | Business question (plain English) | KPI(s) | Grain (per what?) | Dimensions (by what?) | Consumer | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | | | | | | must |

For every KPI: formula in words, numerator/denominator, filters, time window, currency/unit, and **who signs off the number**.

## 4. Business definitions and glossary 🟧

Owner: ______ · Status: ______

| Term | Definition | Source of truth | Ambiguities resolved |
| --- | --- | --- | --- |
| Customer | | | e.g. "individual vs organisation" |
| Order | | | e.g. "re-emitted order = new version, not new order" |
| Net revenue | | | after which discounts, before/after tax |
| Active | | | |

Includes conformed dimension definitions (what is "the" customer / product across sources).

## 5. Source system details 🟥

Owner (per source): ______ · Status: ______

One block per source. **This is the minimum to start implementation.**

| Item | Value |
| --- | --- |
| System, technology, hosting | e.g. Azure SQL, Cosmos DB (Mongo API), S3 |
| Object(s) | table / collection / path |
| Owner and contact | |
| Access method | JDBC / API / SDK / file drop; driver and version |
| Credentials location | secret scope name and key names (never the values) |
| Network prerequisites | firewall rules, private link, IP allow-list |
| Extract type | full / incremental / CDC / event stream |
| Extract window | when the source is complete for the day; time zone |
| Volume and growth | rows, bytes, per day and total |
| Business key | column(s) and whether they are truly unique |
| Change semantics | are rows updated? deleted? re-emitted? versioned? |
| Schema | columns, types, nested structures, nullability |
| Known data issues (from profiling) | |
| Sample data location | |
| Retention at source | how far back can we re-extract |

## 6. Data access, security and secrets 🟥

Owner: ______ · Status: ______

* Deployment identity (dev: user; prod: service principal) and the privileges it needs.
* Where secrets live; naming convention; rotation policy; what to do about credentials ever exposed.
* Consumer access model: which groups get which privileges on which layer (default: `SELECT` on Gold only).
* Row-level / column-level security needs (masking, filters) and the groups exempted.

## 7. Confidentiality classification 🟥

Owner (steward): ______ · Status: ______

| Dataset / column | Classification (public / internal / confidential / PII / special category) | Handling rule | Retention |
| --- | --- | --- | --- |

How classification is *enforced* (tags in the catalog, masks, tests that fail when a tag is lost).

## 8. Data refresh, latency and SLAs 🟧

Owner: ______ · Status: ______

| Item | Value |
| --- | --- |
| Refresh frequency | e.g. daily / hourly / streaming |
| Source ready time → Gold ready time (SLA) | |
| Acceptable staleness for each consumer | |
| Backfill requirements | how many days back, how often |
| Late-arriving data handling | |
| Time zone of business dates | |

## 9. Data quality rules 🟧

Owner: ______ · Status: ______

| Layer | Entity | Rule | Type (not_null, unique, accepted_values, regex, range, min_row_count, expression, freshness, reconciliation) | Severity (error / warn) | On failure (fail / quarantine / warn) | Agreed with |
| --- | --- | --- | --- | --- | --- | --- |

Plus reconciliation acceptance criteria (source → warehouse → report totals) and the
**known caveats accepted by the business** (documented, not hidden).

## 10. Conceptual model 🟧

Owner: ______ · Status: ______

Business entities and relationships in words and one diagram (customer *places* order,
order *contains* product, sale *happens at* store…). No columns, no keys. This is what
you validate with the business.

## 11. Logical model 🟧

Owner: ______ · Status: ______

Entities with attributes, business keys, relationships and cardinalities, history
strategy per entity (SCD type 0/1/2), conformed dimensions, fact grain statements
("one row per order line"). Independent of platform.

## 12. Target design — Gold layer 🟧

Owner: ______ · Status: ______

| Object | Type (dim / fact / view / metric view) | Grain | Keys (PK / FKs) | Measures / attributes | Source entities | Unknown-member rule |
| --- | --- | --- | --- | --- | --- | --- |

Governed metrics list (name, formula, owner). Views for AI/ad-hoc (one-big-table, current-state).

## 13. Physical model 🟩

Owner: ______ · Status: ______

Catalog/schema/table names; storage format (Delta); layer conventions (audit columns,
lineage columns); partitioning or liquid clustering keys; table properties (CDF,
deletion vectors, auto-optimise); constraints declared; write patterns per layer
(replaceWhere by load date, MERGE, full rebuild); naming conventions; environments
(catalog per env or schema per env).

## 14. Ingestion and landing design 🟧

Owner: ______ · Status: ______

Raw landing location and folder convention (`<source>/load_date=`), format kept
(original bytes vs one-time serialisation), idempotency per date, schema evolution
policy (additive allowed, type changes fail), compute constraints (e.g. Serverless
limits).

## 15. Orchestration and frequency 🟩

Owner: ______ · Status: ______

Job/DAG diagram; tasks and dependencies; schedule and time zone; parameters (run date);
retries and timeouts; run-if rules (partial day never loads silently); concurrency;
notifications; maintenance tasks (OPTIMIZE/VACUUM, retention).

## 16. Observability and operations 🟩

Owner: ______ · Status: ______

Run-log table (status, rows, duration, error per entity); structured logging; alerts
(job failure, duration SLA, quality failures, freshness); on-call runbook scenarios
(re-run a day, one source failed, quarantine review, schema change).

## 17. Consumption layer 🟩

Owner: ______ · Status: ______

| Consumer | Tool | Entry point (schema/objects) | Access path (SQL warehouse, API) | Special needs |
| --- | --- | --- | --- | --- |
| BI | Power BI / Tableau | star schema, date table | | relationships from PK/FK |
| AI | Genie / Assistant / notebooks | OBT views, metric views | | comments, keys |
| Finance | SQL | metric views | | reconciliation |

## 18. Data dictionary 🟩

Owner: ______ · Status: ______

Generated from configuration where possible; table and column descriptions, keys,
classification. Location and regeneration command.

## 19. Lineage 🟩

Owner: ______ · Status: ______

How a Gold row traces back to a raw file (run id, load date, source file); system-level
lineage (catalog lineage, Purview/UC lineage); who consumes what.

## 20. Environments, deployment and testing 🟩

Owner: ______ · Status: ______

Dev/test/prod targets; deployment tool (bundles, IaC); CI checks (lint, unit, contract,
bundle validate); approval gates; test families (unit, contract, end-to-end fixtures,
integration); how config changes are reviewed.

## 21. Traceability 🟩

| Requirement / question | Design decision | Artefact (file / table / job) | Test that proves it |
| --- | --- | --- | --- |

## 22. Risks, assumptions, open questions and change log 🟥 (start day 1, never closes)

| Date | Type (risk / assumption / question / change) | Description | Owner | Resolution |
| --- | --- | --- | --- | --- |

## 23. Acceptance and sign-off 🟩

Acceptance questions answered (§3) in each consumer tool; reconciliation passed (§9);
non-functional checks passed (§8, §16); documentation current; sign-off by sponsor and
finance with date.
