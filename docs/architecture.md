# Architecture

## Principles

1. **Code lives next to the layer it belongs to.** `src/ingestion/`, `src/bronze/`,
   `src/silver/` and `src/gold/` each hold their own `code/` and, where it earns its
   place, `config/`; `quality/` and `governance/` sit outside `src/` because they are
   cross-cutting concerns rather than steps in the load. You can open any one of them
   and understand it alone.
2. **Configuration where things repeat, code where they differ.** Ingestion is one
   config per source because the shape is identical. Silver is one notebook per entity
   because the cleaning rules genuinely differ. Gold is SQL because Gold is SQL.
3. **`common_utils` stays generic.** It knows about logging, audit columns, SCD merges,
   renaming, casting, Excel and XML — nothing about retail, customers or orders. It is
   meant to be copied into the next project unchanged. A test enforces this.
4. **Every layer is idempotent per `run_date`.** Re-running any day is safe.
5. **Serverless-first.** No cluster libraries, no JVM bridges, no RDDs, no caching.
   Python clients are declared once in the job environment.
6. **Self-describing outputs.** Comments, tags, constraints and metric views are part
   of the load, not an afterthought, because BI tools and LLM agents read them.

## Layers

| Layer | Object | Write pattern | Keys / time | Consumers |
| --- | --- | --- | --- | --- |
| Raw | `raw_data/<source>/load_date=<d>` | overwrite the date folder | file per extract | replay, audit |
| Bronze | `bronze.<entity>` | `replaceWhere _load_date` | source key + `_load_date` | engineering |
| Silver | `silver.customers`, `silver.sales_orders`, `silver.sales_order_lines`, `silver.sales_order_clicks`, `silver.sales` | SCD1 / SCD2 `MERGE` on `_row_hash` | business key (+ `effective_from`) | engineering, advanced analysts |
| Gold | `gold.dim_*`, `gold.fact_*` | full rebuild from SQL | surrogate keys, PK/FK | BI, AI, business |
| Gold | `gold.*_current`, `gold.*_obt`, `gold.mv_*` | views | – | BI, Genie, notebooks |
| Ops | `ops.pipeline_runs`, `ops.data_quality_results` | append | `run_id` | on-call, freshness |

## Run flow

```
ingest_sqlserver ─┐
ingest_cosmos ────┼─> bronze ─┬─> silver_customers ────┐
ingest_s3 ────────┘           ├─> silver_sales_orders ─┼─> gold ─┬─> quality
                              └─> silver_sales ────────┘         └─> governance
```

* Job parameter `run_date` (default `{{job.start_time.iso_date}}`) flows into every
  task's widget, so one date describes the whole run and a backfill is one parameter.
* Ingestion tasks are independent and retried twice; `bronze` runs only if all three
  succeed, so a partial day is never loaded silently. `gold` likewise waits for all
  three Silver tasks.
* `quality` and `governance` run after Gold and are deliberately terminal: neither can
  break the load.

## Three audit columns, not seven

`_load_date` (which extract this row belongs to — this is what makes reloads
idempotent), `_ingested_at` (when it landed) and `_source_file` (where it came from).
Silver adds `_row_hash` for change detection and `_updated_at`. Anything else was noise
that shipped with every row forever; the run id, environment and task name live in
`ops.pipeline_runs`, which is where you actually look for them.

## Nested data: flatten in Silver

The Cosmos order document carries three arrays. The rule is about grain:

* an array whose elements have their own grain becomes its own table —
  `ordered_products` → `silver.sales_order_lines` (grain: order × line),
  `clicked_items` → `silver.sales_order_clicks` (grain: order × product);
* an array that merely describes the parent stays an attribute — a line's
  `promotion_info` becomes `promo_id`, `promo_discount_rate` and `promo_quantity` on
  the line; the order-level `promo_info` becomes `has_promotion`.

One notebook (`src/silver/code/sales_orders_silver.ipynb`) produces all three tables from
one read, and **de-duplicates the header before exploding**: Cosmos re-sends an order
as a new document when it changes, and an older document can carry lines the new one no
longer has. De-duplicating only the header would leave those orphan lines behind and
the line totals would stop adding up to the header total. See ADR 0005.

## Slowly changing dimensions

* Each batch is de-duplicated on the business key (latest `order_by` wins).
* `_row_hash` = SHA-256 over sorted business columns, null-safe — audit columns are
  excluded so a reload with identical content is a no-op.
* SCD2 closes the changed key (`effective_to`, `is_current = false`) and inserts a new
  version with `effective_from` = batch timestamp; optional `detect_deletes` closes
  keys that vanished from a full extract.
* Gold dimensions derive a version-level surrogate key
  (`xxhash64(business_key, effective_from)`); facts resolve the version valid at the
  event time, giving correct point-in-time joins without extra filters.

## Data quality: detect and report

`quality/config/rules.json` holds 39 rules (`not_null`, `unique`, `accepted_values`,
`regex`, `range`, `min_row_count`, `expression`) across Bronze, Silver and Gold, plus 5
cross-table reconciliations — line totals against header totals, Gold revenue against
Silver revenue, and orphan-key checks against three dimensions.

The quality task runs **after** Gold, writes every result to
`ops.data_quality_results`, and never fails the job. There are no quarantine tables.
That is safe here because of how the layers write: Silver merges on a business key, so
a broken extract merges nothing rather than corrupting a table, and Gold is a full
rebuild from Silver. Blocking the load would therefore only replace a reported problem
with an outage. Alerting is a SQL alert on the results table, where it belongs. See
ADR 0007.

## Security and governance

* Secrets: Databricks secret scope only; a test blocks credential literals.
* PII: declared per table in `governance/config/tables.json`, propagated Bronze →
  Silver → Gold (a contract test fails if Gold loses a tag Silver declared) and tagged
  in Unity Catalog. Column masks and row filters can be attached to the tagged columns.
* Grants: schema-level, declared in the same config; production gives consumers
  `SELECT` on Gold only.
* Delta: change data feed, deletion vectors, auto-optimise and liquid clustering on
  every table.
* Every governance statement is best-effort: a workspace that does not support one of
  them logs a warning instead of failing the run.

## Environments

`dev` (development mode, schedule paused, deploying user) and `prod` (production mode,
schedule on, service principal). Catalog and schema names arrive as job parameters, and
Gold SQL uses `${catalog}` / `${silver}` / `${gold}` placeholders — never a literal
catalog name, which a test enforces.
