# Class 7 — Idempotency: run_date, load_date and replaceWhere

## Objectives

* Define idempotent ("running twice gives the same result as running once") and show why a daily pipeline must be.
* Replace only one day's rows in a Delta table with `replaceWhere`.
* Allow new columns from the source (`mergeSchema`) but fail on type changes.
* Loop over every source in `bronze.json` and build `ds2b` v1.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–15 | Demo of the bug: append twice → duplicates; overwrite → lost history |
| 15–35 | Mini-lesson: idempotency, `replaceWhere` |
| 35–55 | v1: `write_idempotent` function; test with two dates |
| 55–70 | Mini-lesson: schema evolution |
| 70–90 | v2: loop over config → `ds2b` v1 |
| 90–95 | Homework |

## The bug (15 min)

Using yesterday's Class 6 code:

```python
bronze.write.format("delta").mode("append").saveAsTable("retaildataplatform.bronze.customers")
bronze.write.format("delta").mode("append").saveAsTable("retaildataplatform.bronze.customers")
```

```sql
SELECT _load_date, count(*) FROM retaildataplatform.bronze.customers GROUP BY 1;
```

Rows doubled. Now `mode("overwrite")` — rows correct but yesterday's `_load_date` (from
homework) is gone. Neither is acceptable: retries happen every week in production.

## Mini-lesson: idempotency and replaceWhere (20 min)

Write the definition on the board and give non-data examples (pressing a lift button
twice). Then:

```python
(bronze.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", "_load_date = '2026-09-03'")
    .saveAsTable("retaildataplatform.bronze.customers"))
```

Run it three times; the count per `_load_date` never changes. Explain: "overwrite, but
only the rows matching this condition". The `_load_date` column we added in Class 6 is
what makes this possible — audit columns are not decoration.

## v1 — `write_idempotent` (20 min)

The first run has no table yet, so:

```python
def write_idempotent(df, catalog, schema, table, load_date):
    target = f"{catalog}.{schema}.{table}"
    if not spark.catalog.tableExists(target):
        print("creating table:", target)
        df.write.format("delta").mode("overwrite").saveAsTable(target)
        return
    print("replacing load_date", load_date, "in", target)
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_load_date = '{load_date}'")
       .option("mergeSchema", "true")
       .saveAsTable(target))
```

Test protocol (students run each line and predict the count before running):

1. Drop the table. Write for `2026-09-02`. Count.
2. Write for `2026-09-03`. Count (should be the sum).
3. Write for `2026-09-03` again. Count (unchanged).

This exact behaviour is asserted by `tests/test_integration_databricks.py::test_bronze_write_is_idempotent_per_load_date` — show the test; they will write it themselves in Class 22.

## Mini-lesson: schema evolution (15 min)

Add a column to the DataFrame (`withColumn("new_col", F.lit("x"))`) and write with
`mergeSchema` — the table gains the column, old rows have NULL. Now cast `customer_id`
to string and write — it fails. Rule for Bronze: **new columns yes, changed types no**;
a type change is a conversation with the source team, not something a pipeline should
silently absorb.

## v2 — loop over config → `ds2b` v1 (20 min)

```python
import json, uuid
from datetime import date

dbutils.widgets.text("run_date", date.today().isoformat())
run_date = dbutils.widgets.get("run_date")
run_id = str(uuid.uuid4())
with open("src/config/bronze.json") as handle:
    config = json.load(handle)
catalog = config["platform"]["catalog"]
schema = config["platform"]["schema"]

for source in config["sources"]:
    print("=== source:", source["name"])
    raw, path = read_landed_raw(source["name"], source["format"], run_date)
    bronze = with_audit_columns(raw, run_date, run_id, source["name"])
    write_idempotent(bronze, catalog, schema, source["target_table"], run_date)
    print("done:", source["target_table"], bronze.count(), "rows")
```

Add `"target_table"` to each source in `bronze.json` (`customers`, `sales`,
`sales_orders`). Run with today's date. Then run again — counts unchanged. Then run with
yesterday's date (from homework) — both days present.

Compare with the reference `src/bronze/ds2b.ipynb`: the loop is the same; the extra
pieces are quality rules (Class 18), governance (Class 19), `track_entity` (Class 17), and
"keep going when one source fails, report at the end" (Class 17).

## Homework

1. Add `primary_keys` to each source in `bronze.json` (`customer_id`+`valid_from`, `customer_id`+`order_date`, `order_number`) — no code uses them yet; Class 18 will.
2. Backfill: land and load `2026-09-01` for one source; verify three `_load_date` values.
3. Explain in writing why `replaceWhere` needs the `_load_date` column and what would happen if a source row itself had a column named `load_date`.

## Common problems

* `replaceWhere` predicate must match *all* rows being written, otherwise Delta raises "data does not match replaceWhere" — the audit column guarantees it.
* `mergeSchema` + `overwriteSchema` together is not allowed; we never use `overwriteSchema` in Bronze.
* Widget default is today; a student who landed only yesterday's files gets "path is missing" — good moment to show the message from `read_landed_raw` and how a clear error saves time.
