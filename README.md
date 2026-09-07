# Retail Data Platform

A Databricks lakehouse that turns three operational sources (Cosmos DB, Amazon S3,
Azure SQL Server) into a governed Gold star schema for BI and AI consumers. It runs
entirely on **Serverless** compute and deploys with **Databricks Asset Bundles**.

```
 Azure SQL         Cosmos DB          S3 (Parquet)
     │                 │                   │      ingestion/code/*.ipynb  (3 tasks, parallel)
     ▼                 ▼                   ▼
 raw_data volume  /<source>/load_date=YYYY-MM-DD   ── the landed bytes, replayable
     │                                              bronze/code/raw_to_bronze.ipynb
     ▼
 bronze.customers   bronze.sales_orders   bronze.sales     ── as received + 3 audit columns
     │                                              silver/code/*.ipynb  (3 tasks, parallel)
     ▼
 silver.customers          (SCD2)
 silver.sales_orders       (SCD2)  ┐
 silver.sales_order_lines  (SCD1)  ├─ one notebook flattens the order document into 3 tables
 silver.sales_order_clicks (SCD1)  ┘
 silver.sales              (SCD1)
     │                                              gold/code/build_gold.ipynb  (runs gold/sql/*.sql)
     ▼
 gold.dim_date  dim_customer  dim_product  dim_promotion       ── conformed dimensions, Unknown members
 gold.fact_sales_order_line  fact_sales_order  fact_pos_sale   ── declared grain, PK/FK, reconciled
 gold.customers_current  *_obt views  mv_* metric views        ── for Power BI, Genie and agents
     │
 ops.pipeline_runs   ops.data_quality_results   ── run history and quality trends
```

## How the repository is laid out

Every layer owns its code and its configuration, and nothing else:

```
common_utils/     generic library — reusable in any project, in any domain
ingestion/  code/*.ipynb   config/*.json      one notebook + one config per source
bronze/     code/raw_to_bronze.ipynb          one loop: Bronze is identical for every source
silver/     code/*.ipynb                      one notebook per entity, explicit PySpark
gold/       code/build_gold.ipynb  sql/*.sql  numbered SQL files, run in filename order
quality/    code/*.ipynb   config/rules.json  detect and report, never blocks
governance/ code/*.ipynb   config/tables.json comments, tags, PII, PK/FK, grants
resources/  jobs.yml                          the DAG
tests/                                        local Spark, no workspace needed
tools/      nb.py  data_dictionary.py
docs/       architecture, consumers, operations, ADRs, data dictionary
classes/    the bootcamp curriculum built on this project
```

Two rules keep it that way:

* **`common_utils` never learns this project's vocabulary.** Logging, audit columns,
  SCD merges, renaming, casting, Excel/XML ingestion — generic things only. A test
  (`test_common_utils_stays_generic`) fails if a domain word appears in it.
* **Business logic lives in the layer, in the open.** Silver is plain PySpark you can
  read top to bottom; Gold is plain SQL. Nothing is hidden behind a config interpreter.

## What makes it production-grade

| Concern | How it is handled |
| --- | --- |
| Idempotency | Landing overwrites the date folder, Bronze writes with `replaceWhere _load_date`, Silver merges on a row hash, Gold rebuilds. Re-run any day with `--params run_date=2026-09-01`. |
| Change history | SCD Type 2 on customers and order headers, SCD Type 1 where history has no value. A null-safe `_row_hash` decides what actually changed. |
| Data quality | 39 declarative rules plus 5 cross-table reconciliations in `quality/config/rules.json`. Detect-and-report: results land in `ops.data_quality_results` and the pipeline never fails on them — safe because Silver merges, so a broken extract merges nothing. |
| Governance | Comments, tags, PII column tags, owner, informational PK/FK and grants applied from `governance/config/tables.json` on every run, so the catalog cannot drift from the docs. |
| Observability | Structured JSON logs and one row per run/task/entity in `ops.pipeline_runs`. Job email on failure and on a duration SLA breach. |
| Security | Secrets come only from a Databricks secret scope; a test fails if a credential-looking literal is committed. |
| Consumer readiness | Star schema with a date dimension, Unknown members so no fact row is ever dropped, one-big-table views for Genie and agents, metric views for governed measures, and a generated data dictionary. See `docs/consumers.md`. |
| Delivery | `ruff` + `pytest` (local Spark, no workspace) + `databricks bundle validate` in CI; deploy from `main` with an environment approval. |

## Getting started

```bash
pip install -r requirements-dev.txt
ruff check . && pytest                       # lint + unit tests, no workspace needed

databricks bundle validate -t dev
databricks bundle deploy  -t dev
databricks bundle run retail_data_platform -t dev
databricks bundle run retail_data_platform -t dev --params run_date=2026-09-01   # backfill
```

Secrets are read from the scope named by the `secret_scope` job parameter; each
`ingestion/config/*.json` lists the key names, never the values. One-time workspace
setup is in `docs/operations.md`.

Notebooks are `.ipynb`. `tools/nb.py` converts a `# Databricks notebook source` .py
file into one if you prefer to edit as text.

## Extending it

* **New source** — add `ingestion/config/<name>.json`, a notebook in `ingestion/code/`
  if the protocol is new, and a task in `resources/jobs.yml`. Bronze picks it up from
  the config folder automatically.
* **New Silver entity** — add a notebook in `silver/code/` and a task. Read it like a
  script: read Bronze, clean, hash, de-duplicate, merge.
* **New Gold product** — add a numbered `.sql` file in `gold/sql/`, then describe it in
  `governance/config/tables.json` and run `python tools/data_dictionary.py`.

## Where to look next

* `docs/architecture.md` — the design and why each choice was made
* `docs/consumers.md` — how BI and AI engineers should query Gold
* `docs/operations.md` — running, backfilling, and what to do when a task fails
* `docs/adr/` — the decisions, including why this structure replaced the earlier one
* `classes/` — the bootcamp curriculum built on this project
