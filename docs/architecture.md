# Architecture

## Principles

1. **Configuration is the contract.** All behaviour (sources, rules, transformations,
   SCD strategy, star schema, comments, grants) is declared in `src/config/*.json`,
   validated by typed models, and reviewed through pull requests. Python is generic.
2. **Every layer is idempotent per `run_date`.** Re-running any day is safe.
3. **Serverless-first.** No cluster libraries, no JVM bridges, no RDDs. Python clients
   are declared once in the job environment.
4. **Self-describing outputs.** Comments, tags, constraints and metric views are part
   of the load, not an afterthought, because BI tools and LLM agents read them.
5. **Fail loudly, but not all at once.** Bronze/Silver process every entity and
   report all failures; Gold stops at the first failure to avoid a partial star.

## Layers

| Layer | Object | Write pattern | Keys / time | Consumers |
| --- | --- | --- | --- | --- |
| Raw | `bronze.raw_data/<source>/load_date=<d>` | overwrite folder | file per extract | replay, audit |
| Bronze | `bronze.<entity>` (+`_quarantine`) | `replaceWhere _load_date` | source key + `_load_date` | engineering |
| Silver | `silver.<entity>` (headers, flattened child tables such as `sales_order_lines`, `sales_order_clicks`) | SCD1 / SCD2 `MERGE` on `_row_hash` | business key (+ `effective_from`) | engineering, advanced analysts |
| Gold | `gold.dim_*`, `gold.fact_*` | full rebuild | surrogate keys, PK/FK | BI, AI, business |
| Gold | `gold.*_current`, `gold.*_obt`, `gold.mv_*` | views | – | BI, Genie, notebooks |
| Ops | `ops.pipeline_runs`, `ops.data_quality_results` | append | `run_id` | on-call, freshness |

## Run flow

```
land_cosmos ─┐
land_s3 ─────┼─> ds2b ─> b2s ─> s2g
land_sqlserver ┘
```

* Job parameter `run_date` (default `{{job.start_time.iso_date}}`) flows to every task.
* Each landing task is independent and retried twice; `ds2b` runs only if all succeed
  (partial days are never loaded silently).
* `RunContext` (`run_id`, `run_date`, `environment`, `catalog`) is created once per
  notebook and stamped on every row, log line and ops record.

## Data quality model

* Rule types: `not_null`, `unique`, `accepted_values`, `regex`, `range`,
  `min_row_count`, `expression`.
* Severity `error` blocks (or quarantines) the entity; `warn` only records.
* `on_quality_failure` per source: `fail` (default for critical feeds), `quarantine`
  (row-level rules move failing rows to `<entity>_quarantine` with `_failed_rules`),
  `warn`.
* Results are appended to `ops.data_quality_results` for trend dashboards and alerts.

## Nested data: flatten in Silver

A nested array that has its own grain (order lines, click-stream) becomes its own Silver
table via the `explode` transformation (`silver.json`), keyed by parent key + position.
Silver stays queryable without `LATERAL VIEW`, quality rules apply per element, and Gold
facts become plain joins. Arrays that are attributes of a row (a line's promotion) stay
attributes. See ADR 0005.

## Slowly changing dimensions

* Batch de-duplicated on business key (latest `order_by` wins).
* `_row_hash` = SHA-256 over sorted business columns (null-safe) — audit columns are
  excluded so a reload with identical content is a no-op.
* SCD2: changed keys are closed (`effective_to`, `is_current=false`) and a new version
  is inserted with `effective_from` = batch timestamp; optional `detect_deletes` closes
  keys missing from a full extract.
* Gold dimensions derive a version-level surrogate key
  (`xxhash64(business_key, effective_from)`); facts resolve the version valid at the
  event time, giving correct point-in-time joins without extra filters.

## Security and governance

* Secrets: Databricks secret scope only; CI blocks credential literals.
* PII: declared per entity (`pii_columns`), propagated Bronze -> Silver -> Gold (contract
  test) and tagged in Unity Catalog. Column masks / row filters can be attached to the
  tagged columns via `governance.py` when consumer groups exist.
* Grants: schema-level, declared in config; production gives consumers `SELECT` on Gold only.
* Delta: change data feed, deletion vectors, auto-optimise, liquid clustering on all tables.

## Environments

`dev` (development mode, schedule paused, deploying user) and `prod` (production mode,
schedule on, service principal). Catalog names are variables; SQL in config uses
`${catalog}` / `${silver}` / `${gold}` placeholders and never literal names.
