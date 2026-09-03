# Class 26 — Capstone: add a fourth source end to end

Students prove the platform is extensible by adding a new source without touching
`common_utils/` — only configuration, a job task, tests and docs. Run as a workshop with
a demo at the end; the rubric doubles as the course assessment.

## The brief

Add a **returns** feed (or any small dataset the instructor provides as a CSV in a
volume, an S3 object, or a SQL table): `return_id, order_number, product_id, returned_qty,
return_date, reason`. Deliver it through every layer:

1. **Bronze**: source entry in `bronze.json` (type, secret keys or path, `target_table`, `primary_keys`, `pii_columns`, quality rules with sensible severities, `on_quality_failure`).
2. **Landing**: a `land_returns` task in `jobs.yml` (and `ds2b` depends on it).
3. **Silver**: entity in `silver.json` (transformations, SCD type with a justification, `order_by`, column comments, rules).
4. **Gold**: `fact_return` with FKs to `dim_product`, `dim_customer` (via the order) and `dim_date`; a `returns_obt` view; a `return_rate` measure added to `mv_web_sales` or a new `mv_returns`.
5. **Tests**: fixture CSV in `tests/fixtures/`, extend the end-to-end test (PK, FKs, a reconciliation), contract tests must pass unchanged.
6. **Docs**: regenerate the data dictionary; add the grain statement to `consumers.md`; write ADR 0006 for the SCD choice.
7. **CI**: open a PR; CI green; deploy to dev; run the job; show `ops.pipeline_runs` and `ops.data_quality_results`.

## Time plan (100 min + homework time)

| Min | Segment |
| --- | --- |
| 0–10 | Brief, pairs, rubric |
| 10–70 | Build (instructor circulates; hints below) |
| 70–90 | Demos: each pair shows the job run and one query on Gold |
| 90–100 | Retrospective: what was hard, what the platform made easy |

## Hints (release one at a time)

* Look at `Source.from_dict` for the required keys of your source type.
* `sale_id`-style hash keys are fine when the source has no id.
* A fact that references an order needs the order's customer: join `silver.sales_orders` (current) in the Gold SQL.
* `depends_on` must list `fact_sales_order_line` if you join it.
* Run `pytest -q` before every deploy; the contract tests will name what you forgot.

## Rubric (100 points)

| Area | Points | Evidence |
| --- | --- | --- |
| Configuration correctness | 20 | `load_*_config()` passes; keys, rules, comments present |
| Data quality choices | 15 | severities justified; quarantine used where appropriate; results visible in ops |
| Silver design | 15 | correct SCD type with reasoning; transformations handle the feed's quirks |
| Gold modelling | 15 | grain statement; FKs to conformed dims; Unknown members respected; reconciliation holds |
| Tests | 15 | fixture + assertions; suite green locally and in CI |
| Operations | 10 | job task, dependency, successful run, `ops.pipeline_runs` row |
| Documentation | 10 | dictionary regenerated, consumer note, ADR |

Stretch goals (from Class 25's gap list) earn a distinction, not points.

## What students should be able to say afterwards

"I can take a new source from raw files to a governed, tested, documented star schema by
editing three JSON files and one YAML file, because the platform's behaviour lives in
configuration and the code is generic, tested and observable."
