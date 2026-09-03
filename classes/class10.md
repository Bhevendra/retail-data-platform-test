# Class 10 — SCD theory + SCD Type 1 with MERGE

## Objectives

* Explain slowly changing dimensions: why history matters and the difference between Type 1 and Type 2.
* Write a Delta `MERGE INTO` statement in SQL and read what each clause does.
* Compute a row hash for change detection and explain why it must be null-safe and order-stable.
* Implement `merge_type_1` and prove it is idempotent.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–30 | Mini-lesson: SCD (the 30-minute theory block) |
| 30–50 | Mini-lesson: MERGE on a toy table |
| 50–70 | Row hash: why and how |
| 70–90 | `merge_type_1` for `sales`; idempotency test |
| 90–100 | Homework |

## Mini-lesson: slowly changing dimensions (30 min)

Story: customer 1003 moves from Colchester to Boston in June. A report of "sales by
city for March" must still say Colchester. Draw the three options:

| Type | What happens on change | Report for March says | Use when |
| --- | --- | --- | --- |
| 0 | ignore the change | Colchester (stale) | values must never change |
| 1 | overwrite in place | Boston (wrong for March) | corrections, no history needed |
| 2 | close old row, insert new row | Colchester ✔ | analysis over time |

Type 2 columns on the board: `effective_from`, `effective_to`, `is_current`. Walk through
three days of loads for one customer by hand, filling a table on the board. Then ask
which entities in our project need which type: `sales` (a receipt never changes → Type 1,
mostly for de-duplication), `customers` and `sales_orders` (versions matter → Type 2).

Natural break here if the group needs it.

## Mini-lesson: MERGE (20 min)

```sql
CREATE OR REPLACE TABLE retaildataplatform.bronze.demo_target AS
SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, v);
CREATE OR REPLACE TEMP VIEW demo_source AS
SELECT * FROM VALUES (2, 'B'), (3, 'c') AS t(id, v);

MERGE INTO retaildataplatform.bronze.demo_target t
USING demo_source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM retaildataplatform.bronze.demo_target ORDER BY id;
```

Read it aloud as English: "for each source row, find the target row with the same id;
if found update it, if not insert it". Run the MERGE again — nothing changes: MERGE is
naturally idempotent for Type 1. Show `WHEN MATCHED AND <condition>` next; we need it
for the hash.

## Row hash (20 min)

Problem: with `UPDATE SET *` every matching row is rewritten even if identical — slow,
and it hides *whether* anything changed. Solution: a fingerprint of the business columns.

```python
from pyspark.sql import functions as F

def row_hash(df, columns):
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in sorted(columns)]
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))
```

Three design decisions to explain with examples:

* `sorted(columns)` — same hash whatever order the config lists columns in.
* `coalesce(..., "∅")` — NULL and empty string must hash differently (show both).
* Only business columns — audit columns (`_ingested_at`, `_run_id`) change every run and must not be in the hash, or every row would look "changed".

```python
s = spark.table("retaildataplatform.bronze.sales")
business = [c for c in s.columns if not c.startswith("_") and c not in ("last_update_ts", "file_path")]
hashed = row_hash(s, business)
display(hashed.select("customer_id", "order_date", "_row_hash").limit(5))
print("rows:", hashed.count(), "distinct hashes:", hashed.select("_row_hash").distinct().count())
```

The difference is the 8 duplicate receipts from Class 8.

## `merge_type_1` (20 min)

```python
def merge_type_1(source_df, target, keys):
    if not spark.catalog.tableExists(target.replace("`", "")):
        print("first load, creating", target)
        source_df.write.format("delta").saveAsTable(target)
        return
    source_df.createOrReplaceTempView("_scd1_source")
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    spark.sql(f"""
        MERGE WITH SCHEMA EVOLUTION INTO {target} t
        USING _scd1_source s ON {condition}
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
```

Explain `<=>` (null-safe equals), `" AND ".join(...)` building the condition from a list
of keys, and `WITH SCHEMA EVOLUTION` (new source columns are added to the target
automatically — Silver's version of Class 7's `mergeSchema`).

Test protocol with the `sales` Silver DataFrame from Class 9 (key `sale_id`):

1. Drop `silver.sales`; run → created.
2. Run again → `DESCRIBE HISTORY` shows a MERGE with 0 updated, 0 inserted.
3. Change one row's `total_amount` in the source DataFrame and run → 1 updated.

## Homework

1. Explain in writing why `WHEN MATCHED AND t._row_hash <> s._row_hash` is better than plain `WHEN MATCHED`.
2. Draw the SCD2 table for customer 1003 across three loads (address change on load 2, no change on load 3) with `effective_from/to` and `is_current` — you will implement it next class.
3. Read `common_utils/scd.py::merge_type_1` and list every difference from the class version.

## Common problems

* `tableExists` wants an unquoted name; `MERGE` is happiest with backticks — hence the `.replace("`", "")`.
* Duplicate keys in the *source* make MERGE fail ("multiple source rows matched") — Class 11 solves it with de-duplication before the merge.
* Forgetting to include `_row_hash` in the first-load write means the next MERGE cannot compare — the hash column is part of the Silver contract.
