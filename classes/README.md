# Retail Data Platform — Bootcamp Curriculum

A step-by-step course that takes a beginner (basic Python + basic PySpark/SQL) to
building, testing and deploying the full **retail-data-platform** repository on
Databricks Serverless.

## Teaching method (used in every class)

Every new idea follows the same ladder, and every class file is written around it:

1. **Hard-coded script** — values typed straight into the cell, runs top to bottom.
2. **Wrap in a function** — same code, parameters replace the hard-coded values.
3. **Make it talk** — prints that show what each parameter is and what happened.
4. **Move values to config** — read `config.json`, pass values from there.
5. **Make it safe** — logging instead of prints, `try / except`, clear errors.
6. **Make it reusable** — move the function to `common_utils/<file>.py` and import it.

A prerequisite concept (SCD, MERGE, window functions, pytest, YAML …) is taught in a
20–30 minute mini-lesson *inside* the class where it is first needed, never earlier.

Each class file contains: objectives, the mini-lessons to teach first, the live-coding
script in versions (v1 → v2 → v3 …), checkpoints ("everyone should now see …"),
exercises, homework, the real errors students will hit and how to read them, and a
minute-by-minute plan for a **120 minute** session.

## The end state

The repository students end with (identical to the reference repo):

```
common_utils/   logger, metadata, ingestors, writers, transforms, scd, quality, governance, observability, settings
src/ingestion/  code/*.ipynb  config/*.json        src/bronze/code/raw_to_bronze.ipynb
src/silver/     code/*.ipynb (one per entity)      src/gold/code/build_gold.ipynb  src/gold/sql/*.sql
quality/  governance/   (code/ + config/, their own tasks)
resources/jobs.yml   databricks.yml   tests/   docs/   .github/workflows/validate.yml
```

## Phases and classes

| # | Class | What students can do afterwards |
| --- | --- | --- |
| **Phase 0 — Orientation** | | |
| 0 | [The Data Product Blueprint: template + filled example](class00-blueprint.md) | Use the reusable [Data Product Blueprint template](templates/DATA-PRODUCT-BLUEPRINT.md) (23 sections, three gates); fill it with stakeholders; know the minimum needed to start (source details, access, classification) |
| **Phase 1 — Ingestion and Bronze (as taught)** | | |
| 1 | [Ingestion from multiple sources (hard-coded)](class01.md) | Create catalog/schema/volume; land SQL Server as CSV and Cosmos DB as JSON under `load_date=`; explain "land raw first" |
| 2 | [S3 ingestion and raw → Bronze Delta tables](class02.md) | Read Parquet from S3; turn all three landed feeds into Bronze Delta tables with `last_update_ts` and `file_path` |
| 3 | [From six copied notebooks to one function library](class03.md) | logging instead of print; `common_utils` package; ingestion functions; `bronze_ingestor()` + one Bronze notebook; config as JSON |
| 4 | [Secrets and parameters](class05.md) *(to be re-cut)* | Replace credentials with a secret scope; `run_date` as a widget instead of a hard-coded date |
| **Phase 2 — Bronze, properly** | | |
| 5 | [Idempotency: run_date, load_date and replaceWhere](class07.md) *(to be re-cut)* | Re-run a day safely; why `overwrite` on a whole table is not idempotent per date |
| **Phase 3 — Silver** | | |
| 8 | [Data profiling workshop](class08.md) | Find duplicates, broken JSON, epoch timestamps, NULL literals; write a findings list |
| 9 | [Transformations driven by config](class09.md) | rename/cast/parse_json/derived as a loop over a dictionary; tolerant casts |
| 9b | [Nested data: when an array deserves its own table](class09b-nested-data-to-tables.md) | Decide by grain; `explode` step; build `sales_order_lines` / `sales_order_clicks`; reconcile lines against headers |
| 10 | [SCD theory + SCD Type 1 with MERGE](class10.md) | Explain SCD1/2, write MERGE, hash rows for change detection |
| 11 | [SCD Type 2 implementation](class11.md) | effective_from/to, is_current, delete detection, de-duplication with window functions |
| 12 | [Assembling the Silver notebook](class12.md) | Config-driven `b2s` for all three entities |
| **Phase 4 — Gold** | | |
| 13 | [Dimensional modelling](class13.md) | Facts, dimensions, grain, surrogate keys, Unknown members, date dimension |
| 14 | [Building the dimensions](class14.md) | dim_date, dim_customer (SCD2), dim_product from exploded arrays |
| 15 | [Facts, views and the Gold builder](class15.md) | Facts with FKs, one-big-table views, dependency ordering, reconciliation checks |
| **Phase 5 — Engineering practices** | | |
| 16 | [Modular programming: common_utils](class16.md) | Modules, packages, imports, sys.path, typed config with dataclasses |
| 17 | [Logging, exceptions and observability](class17.md) | logging module, JSON logs, try/except/raise, context managers, `ops.pipeline_runs` |
| 18 | [Data quality engine](class18.md) | Rule types, severity, fail/quarantine/warn, `ops.data_quality_results` |
| 19 | [Data governance in Unity Catalog](class19.md) | Comments, tags, PII tags, PK/FK, table properties, liquid clustering, grants |
| 20 | [Orchestration with Databricks Jobs (jobs.yml)](class20.md) | Tasks, dependencies, parameters, retries, schedules, notifications |
| 21 | [Databricks Asset Bundles (databricks.yml)](class21.md) | Targets, variables, deploy, run, backfill |
| 22 | [Testing from zero to the project's test suite](class22.md) | pytest, fixtures, local Spark tests, contract tests, end-to-end fixtures, integration tests |
| 23 | [Git, GitHub Actions and CI/CD](class23.md) | Branches, PRs, lint, tests, bundle validate, gated deploy |
| 24 | [Semantic layer for BI and AI](class24.md) | Metric views, Genie, Power BI relationships, data dictionary, consumer docs |
| 25 | [Enterprise readiness](class25.md) | Runbook, ADRs, alerts, maintenance, masking, service principals, "deploy anywhere" |
| 26 | [Capstone: add a fourth source end to end](class26.md) | Prove the platform is extensible; assessment rubric |

## Prerequisite knowledge assumed on day 1

Python: variables, data types, `for`, `if/else`, functions with a few parameters.
PySpark and SQL: read, select, filter, update/delete, add/rename/drop columns, sort,
dedupe, NULL handling, group/count, joins, union, casting, string and date functions,
`CASE WHEN`.

Everything else (dictionaries as configuration, list comprehensions, `import`,
classes/dataclasses, `try/except`, window functions, MERGE, YAML, pytest, git) is
introduced in the class where it is needed and marked **Mini-lesson** in the file.

## Data used throughout

The three demo sources of the reference project (Cosmos DB `sales_orders`, S3
`sales.parquet`, Azure SQL `retail.customers`). Students who cannot access the live
sources use the CSV copies (`customers.csv`, `sales.csv`, `sales_orders.csv`) uploaded
to a volume in Class 1; every class works with either.

## Suggested pacing

One class per session (**120 min**). The dense classes carry a marked "natural break" —
stop there and start the next session from that point rather than rushing the second half.

## Re-sequencing note (September 2026)

Classes 1–3 have been rewritten to match what was actually delivered in the live sessions
(two hard-coded ingestion/Bronze classes, then one modularisation class). Classes 4
onwards in the table below still carry their original numbering and assume the earlier
split of the same material; they are being re-cut in order. Where a row says
*(to be re-cut)* the content is sound but the class number and the code it starts from
need adjusting.
