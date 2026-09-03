# Retail Data Platform

Config-driven Databricks lakehouse that turns three operational sources
(Cosmos DB, Amazon S3, Azure SQL Server) into a governed, self-describing Gold
star schema for BI and AI consumers. Runs entirely on **Serverless** compute and
deploys with **Databricks Asset Bundles**.

```
 Cosmos DB          S3 (Parquet)        Azure SQL
     │                   │                  │           land_source (x3, parallel)
     ▼                   ▼                  ▼
 bronze.raw_data volume  ── original bytes / one-time serialisation, load_date=YYYY-MM-DD
     │                                                  ds2b
     ▼
 bronze.<entity>         ── audit columns, quality rules, quarantine, idempotent per load date
     │                                                  b2s
     ▼
 silver.<entity>         ── typed, renamed, de-duplicated, SCD1 / SCD2 history, CDF on
     │                                                  s2g
     ▼
 gold.dim_date, dim_customer, dim_product            ── conformed dimensions (PK, comments, Unknown members)
 gold.fact_sales_order_line, fact_sales_order, fact_pos_sale ── facts with FKs, declared grain, reconciled measures
 gold.customers_current, *_obt views                  ── current-state and one-big-table views for AI / Genie
 gold.mv_web_sales, mv_pos_sales                     ── metric views: governed measures (semantic layer)
     │
 ops.pipeline_runs, ops.data_quality_results   ── run history, freshness, quality trends
```

## What makes it production-grade

| Concern | How it is handled |
| --- | --- |
| Configuration as contract | `src/config/*.json` are validated by typed models (`common_utils/config.py`) before any Spark work; CI fails on a bad config. |
| Idempotency | Raw landing, Bronze (`replaceWhere` on `_load_date`), Silver (hash-based SCD merges) and Gold (full rebuild) can all be re-run for any date without duplicates. Backfill = `--params run_date=YYYY-MM-DD`. |
| Data quality | Declarative rules (`not_null`, `unique`, `accepted_values`, `regex`, `range`, `min_row_count`, `expression`), `error`/`warn` severity, per-source `fail` / `quarantine` / `warn` behaviour, every result stored in `ops.data_quality_results`. |
| Observability | Structured JSON logs and one row per run/task/entity in `ops.pipeline_runs` (status, rows, duration, error). Job emails on failure and on duration SLA breach. |
| Governance | Every table gets comments, tags, PII column tags, owner, Delta table properties (change data feed, deletion vectors, auto-optimise), liquid clustering and, in Gold, PK/FK constraints. |
| Security | Secrets only from a Databricks secret scope; tests fail if a credential-looking literal is committed; least-privilege schema grants are declared in config. |
| Consumer readiness | Star schema + date dimension, informational constraints for BI join detection, column-level documentation and metric views for Genie / LLM agents, a generated data dictionary. See `docs/consumers.md`. |
| Delivery | `ruff` + `pytest` (local Spark, no workspace needed) + `databricks bundle validate` in CI; deploys from `main` via the bundle with environment approvals. |

## Repository layout

```
common_utils/      library (unit-tested; Serverless-safe)
  config.py           typed config models + validation
  runtime.py          RunContext, widgets, JSON logging
  sources.py          connectors + raw landing
  bronze.py           raw -> Bronze (audit, quality, quarantine, idempotent write)
  quality.py          rule engine + results table
  scd.py              SCD1/SCD2 merges
  silver.py           transformations + merge orchestration
  gold.py             star schema builder, metric views, data dictionary generator
  governance.py       comments, tags, constraints, properties, grants
  observability.py    ops.pipeline_runs
src/
  config/             bronze.json, silver.json, gold.json  <- the contract
  ingestion/land_source.ipynb   one parameterised landing notebook (source_name)
  bronze/ds2b.ipynb      silver/b2s.ipynb      gold/s2g.ipynb
resources/jobs.yml    Serverless job: 3 landing tasks -> ds2b -> b2s -> s2g
tests/                unit tests (local Spark) + Databricks integration tests
docs/                 architecture, consumers guide, operations runbook, data dictionary, ADRs
```

## Quick start

```bash
pip install -r requirements-dev.txt
ruff check . && pytest                     # < 30 s, no workspace needed

databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_data_platform -t dev                      # today
databricks bundle run retail_data_platform -t dev --params run_date=2026-09-01   # backfill a date
```

One-time workspace setup (secret scope, grants) is in `docs/operations.md`.

## Running a single step interactively

Every notebook has widgets (`run_date`, `catalog`, `secret_scope`, and an optional
subset such as `sources`, `entities`, `products`), so you can run `ds2b` for just
`customers` on a given date without touching the job.

## Extending the platform

* **New source**: add an entry to `src/config/bronze.json` and a `land_<name>` task in
  `resources/jobs.yml` (the contract test will remind you). No Python changes needed.
* **New Silver entity / transformation**: edit `src/config/silver.json`.
* **New Gold product**: add a `table`, `view` or `metric_view` to `src/config/gold.json`
  with `primary_key`, `foreign_keys` and `column_comments`; run `python -m common_utils.gold`
  to regenerate the data dictionary.

Design decisions are recorded in `docs/adr/`.
