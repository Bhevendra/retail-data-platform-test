# Class 11 — SCD Type 2 implementation

## Objectives

* De-duplicate a batch to one row per key using a window function (`row_number`).
* Implement SCD2 as two MERGE statements: close changed versions, insert new versions.
* Add optional delete detection (`WHEN NOT MATCHED BY SOURCE`).
* Verify history on a tiny dataset and then on `customers`.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: the SCD2 table drawn in homework |
| 10–30 | Mini-lesson: window functions and `row_number` |
| 30–45 | `deduplicate(df, keys, order_by)` |
| 45–80 | `merge_type_2` step by step on a toy table |
| 80–95 | Run on `customers`; delete detection |
| 95–100 | Homework |

## Mini-lesson: window functions (20 min)

Start with SQL they can read:

```sql
SELECT customer_id, valid_from,
       row_number() OVER (PARTITION BY customer_id ORDER BY valid_from DESC) AS rn
FROM retaildataplatform.bronze.customers
WHERE customer_id IN (16723, 111595)
ORDER BY customer_id, rn;
```

"For each customer (partition), number the rows newest first (order)". `rn = 1` is the
latest version. Then the PySpark form:

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

w = Window.partitionBy("customer_id").orderBy(F.col("valid_from").desc())
latest = c.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")
```

## `deduplicate` (15 min)

```python
def deduplicate(df, keys, order_by):
    ordering = [F.col(order_by).desc_nulls_last()] if order_by in df.columns else []
    if "_row_hash" in df.columns:
        ordering.append(F.col("_row_hash"))       # deterministic tie-break
    if not ordering:
        return df.dropDuplicates(keys)
    w = Window.partitionBy(*keys).orderBy(*ordering)
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")
```

Why the tie-break: two versions with the same `order_by` value must still pick the same
one every run (idempotency again). Test: `customers` 28,813 → 28,670 rows; `sales_orders`
4,074 → 4,000 with `order_by = "source_document_id"`.

## `merge_type_2` on a toy table (35 min)

Build the reference implementation in three cells, running after each and inspecting the
table. Use a toy source so every effect is visible:

```python
from datetime import datetime, timezone

def merge_type_2(source_df, target, keys, detect_deletes=False):
    batch_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    versioned = (source_df
        .withColumn("effective_from", F.lit(batch_ts).cast("timestamp"))
        .withColumn("effective_to", F.lit("9999-12-31 00:00:00").cast("timestamp"))
        .withColumn("is_current", F.lit(True)))
    if not spark.catalog.tableExists(target.replace("`", "")):
        versioned.write.format("delta").saveAsTable(target)
        return
    versioned.createOrReplaceTempView("_scd2_source")
    condition = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    delete_clause = (f"WHEN NOT MATCHED BY SOURCE AND t.is_current = true THEN UPDATE SET is_current = false, effective_to = timestamp'{batch_ts}'"
                     if detect_deletes else "")
    # Step 1: close current versions whose content changed (or whose key vanished)
    spark.sql(f"""
        MERGE INTO {target} t USING _scd2_source s ON {condition} AND t.is_current = true
        WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET is_current = false, effective_to = timestamp'{batch_ts}'
        {delete_clause}
    """)
    # Step 2: insert new versions for new keys or keys whose current row was just closed
    spark.sql(f"""
        MERGE WITH SCHEMA EVOLUTION INTO {target} t
        USING _scd2_source s ON {condition} AND t.is_current = true AND t._row_hash = s._row_hash
        WHEN NOT MATCHED THEN INSERT *
    """)
```

Toy protocol (students predict before each run):

```python
day1 = row_hash(spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string"), ["id", "v"])
merge_type_2(day1, "retaildataplatform.bronze.demo_scd2", ["id"])
merge_type_2(day1, "retaildataplatform.bronze.demo_scd2", ["id"])          # identical batch -> still 2 rows
day2 = row_hash(spark.createDataFrame([(1, "a2"), (3, "c")], "id int, v string"), ["id", "v"])
merge_type_2(day2, "retaildataplatform.bronze.demo_scd2", ["id"], detect_deletes=True)
```

Expected: id 1 has two rows (old closed, new current), id 2 closed (missing from full
extract), id 3 new. Why two MERGEs? The second one's `ON` includes the hash, so a key
whose current row was just closed is "not matched" and gets inserted; an unchanged key
is matched and ignored. Draw it.

Why the `9999-12-31` high date instead of NULL for `effective_to`? BI tools can do
`BETWEEN` without `COALESCE`.

## `customers` for real (15 min)

```python
silver_customers = deduplicate(row_hash(apply_transformations(bronze_customers, t_customers), business_cols), ["customer_id"], "source_valid_from")
merge_type_2(silver_customers, "retaildataplatform.silver.customers", ["customer_id"])
```

```sql
SELECT is_current, count(*) FROM retaildataplatform.silver.customers GROUP BY 1;
```

Run twice; counts unchanged. Then modify one customer's city in the source and run
again → one closed row, one new current row.

## Homework

1. Write the SQL that returns the version of customer 1003 valid on a given date using `effective_from`/`effective_to`.
2. When would you turn `detect_deletes` on? When would it be dangerous? (Hint: partial extracts.)
3. Run `sales_orders` through `deduplicate` + `merge_type_2` with key `order_number`, order `source_document_id`.

## Common problems

* "Multiple source rows matched" → forgot `deduplicate` before the merge.
* Step 2 inserts nothing on a rerun — correct; check `DESCRIBE HISTORY` operation metrics.
* Timezones: `effective_from` is UTC by construction; never compare it to local dates without converting.
