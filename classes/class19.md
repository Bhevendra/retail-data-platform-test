# Class 19 — Data governance in Unity Catalog

## Objectives

* Apply table/column comments, tags and PII column tags from config.
* Add informational PK/FK constraints in Gold and explain what BI tools do with them.
* Set Delta table properties (change data feed, deletion vectors, auto-optimise) and liquid clustering.
* Grant least-privilege access from config; know what a runtime may refuse and how `govern_table` copes.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–15 | Governance as "metadata that makes data usable"; the consumer's view |
| 15–35 | Comments and tags: hand-written SQL → `apply_comments`, `apply_tags`, `tag_pii_columns` |
| 35–50 | PK/FK constraints; the CASCADE story |
| 50–65 | Table properties and liquid clustering |
| 65–80 | Grants; best-effort statements; `govern_table` bundle |
| 80–95 | Propagation of PII through layers + the contract test |
| 95–100 | Homework |

## Comments and tags (20 min)

SQL first:

```sql
COMMENT ON TABLE retaildataplatform.gold.dim_customer IS 'Customer dimension (SCD2)...';
ALTER TABLE retaildataplatform.gold.dim_customer ALTER COLUMN customer_sk COMMENT 'Surrogate key, unique per version';
ALTER TABLE retaildataplatform.gold.dim_customer SET TAGS ('layer' = 'gold', 'domain' = 'retail');
ALTER TABLE retaildataplatform.gold.dim_customer ALTER COLUMN customer_name SET TAGS ('classification' = 'pii');
```

Show them in Catalog Explorer, then in `information_schema.column_tags`. Then the
functions in `common_utils/governance.py` that generate these from `column_comments`,
`tags`, `pii_columns` in config. Two details worth the class's time: `_sql_string`
escapes quotes in comments; missing columns are skipped with a warning, not an error.

Real error from the project: `COMMENT ON VIEW` does not exist (views use `COMMENT ON
TABLE`), and `ALTER VIEW … ALTER COLUMN … SET TAGS` was rejected by the parser. Hence
`_best_effort` for view column metadata.

## Constraints (15 min)

```sql
ALTER TABLE retaildataplatform.gold.dim_customer ALTER COLUMN customer_sk SET NOT NULL;
ALTER TABLE retaildataplatform.gold.dim_customer ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk);
ALTER TABLE retaildataplatform.gold.fact_sales_order_line ADD CONSTRAINT fk_fact_sales_order_line_dim_customer
  FOREIGN KEY (customer_sk) REFERENCES retaildataplatform.gold.dim_customer (customer_sk);
```

Unity Catalog does not *enforce* PK/FK (our quality rules do), but Power BI builds
relationships from them and Genie uses them to write joins. Because Gold is rebuilt each
run, `apply_constraints` drops and re-adds: dropping a PK that an FK references needs
`CASCADE`, and facts (built after dims) re-add their FKs — trace this in the code.

## Table properties and clustering (15 min)

```sql
ALTER TABLE retaildataplatform.silver.customers SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true', 'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');
ALTER TABLE retaildataplatform.silver.customers CLUSTER BY (customer_id);
SELECT * FROM table_changes('retaildataplatform.silver.customers', 1) LIMIT 5;
```

Change data feed = "give me only what changed since version N" (feature stores, reverse
ETL). Liquid clustering = "sort files by these columns" without the rigidity of
partitions; `cluster_by` in config comes from the keys.

## Grants and best effort (15 min)

`platform.grants` in config, e.g. `{"SELECT": ["bi_readers"]}` → `GRANT SELECT ON SCHEMA`.
Groups may not exist yet (Free Edition), ownership transfer may be refused — so
`apply_grants`, `set_owner`, `set_clustering` log a warning instead of failing a load.
Discuss the principle: **data loads must never fail because of metadata**, but metadata
failures must be visible in logs.

`govern_table(...)` bundles everything; every layer calls it after writing. Read it.

## PII propagation (15 min)

`pii_columns` is declared in Bronze, must survive Silver renames and drops, and reach
Gold. `tests/test_contracts.py::test_pii_columns_declared_in_bronze_stay_declared_downstream`
enforces it — break it (remove `street` from Silver `customers`) and watch the test fail.
This is governance as code: a reviewer cannot merge a PR that loses a PII tag.

## Homework

1. Add a column comment for every column of `fact_pos_sale` in `gold.json`; rebuild; check Catalog Explorer.
2. Query `system.information_schema.column_tags` for all `pii` columns in the catalog.
3. Read `docs/consumers.md` "AI / ML engineers" and explain how a feature pipeline would exclude PII automatically.

## Common problems

* `ALTER TABLE ... CLUSTER BY` fails on tables with partitions — ours have none.
* Constraint names must be unique per table; we derive them from table + referenced table.
* `SET TAGS` requires `APPLY TAG` privilege in shared workspaces — another reason it is best-effort in some places.
