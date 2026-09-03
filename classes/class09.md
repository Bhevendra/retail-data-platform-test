# Class 9 — Transformations driven by config

## Objectives

* Implement each Silver fix from Class 8 as PySpark code for `customers` (hard-coded).
* Convert the fixes into a loop over a dictionary: `rename`, `cast`, `trim`, `parse_json`, `derived`, `drop`, `null_literals`, `filter`.
* Explain why a fixed *order* of steps matters.
* Explain ANSI mode and write a tolerant cast (`try_cast`).

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap the findings list |
| 10–35 | v1: `customers` fixes hard-coded |
| 35–50 | Mini-lesson: dictionaries as instructions; order of operations |
| 50–75 | v2: `apply_transformations(df, t)` |
| 75–90 | Mini-lesson: ANSI mode and `try_cast` (the real failure) |
| 90–100 | Homework |

## v1 — hard-coded fixes (25 min)

```python
from pyspark.sql import functions as F
c = spark.table("retaildataplatform.bronze.customers").filter("_load_date = '2026-09-03'")

for name in ["postcode", "city", "unit", "region", "district", "tax_id", "tax_code"]:
    c = c.withColumn(name, F.when(F.trim(F.col(name)).isin(["", "NULL", "null", "N/A", "NA", "NA NA", "nan"]), None).otherwise(F.col(name)))

c = (c.withColumnRenamed("valid_from", "source_valid_from")
      .withColumnRenamed("valid_to", "source_valid_to")
      .withColumn("customer_id", F.col("customer_id").cast("bigint"))
      .withColumn("source_valid_from", F.col("source_valid_from").cast("bigint"))
      .withColumn("customer_name", F.expr("regexp_replace(trim(customer_name), '\\\\s+', ' ')"))
      .withColumn("customer_type", F.expr("CASE WHEN customer_name LIKE '%,%' THEN 'individual' ELSE 'organisation' END"))
      .withColumn("last_name", F.expr("CASE WHEN customer_name LIKE '%,%' THEN trim(split_part(customer_name, ',', 1)) END"))
      .withColumn("first_name", F.expr("CASE WHEN customer_name LIKE '%,%' THEN trim(split_part(customer_name, ',', 2)) END"))
      .withColumn("postcode", F.expr("nullif(regexp_replace(postcode, '\\\\.0$', ''), '0')"))
      .withColumn("source_valid_from_ts", F.expr("CAST(source_valid_from AS TIMESTAMP)"))
      .withColumn("is_active", F.expr("source_valid_to IS NULL")))
display(c.select("customer_id", "customer_name", "customer_type", "first_name", "last_name", "postcode", "source_valid_from_ts", "is_active").limit(10))
```

`F.expr("...")` is new: "write the expression in SQL, get a column back". Students know
the SQL; this just lets them use it inside PySpark.

## Mini-lesson: a dictionary of instructions (15 min)

Look at v1: every line is one of a few *kinds* of step with different *values*. Kinds
go in code, values go in config:

```python
transformations = {
    "null_literals": ["", "NULL", "null", "N/A", "NA", "NA NA", "nan"],
    "rename": {"valid_from": "source_valid_from", "valid_to": "source_valid_to"},
    "cast": {"customer_id": "bigint", "source_valid_from": "bigint", "source_valid_to": "bigint", "lon": "double", "lat": "double"},
    "derived": {
        "customer_name": "regexp_replace(trim(customer_name), '\\\\s+', ' ')",
        "customer_type": "CASE WHEN customer_name LIKE '%,%' THEN 'individual' ELSE 'organisation' END",
        "postcode": "nullif(regexp_replace(postcode, '\\\\.0$', ''), '0')",
        "source_valid_from_ts": "CAST(source_valid_from AS TIMESTAMP)",
        "is_active": "source_valid_to IS NULL"
    }
}
```

Why order matters (board): null literals **before** cast (else `'NULL'` breaks the cast),
rename **before** cast (so config uses the new names), cast **before** derived (so
expressions see typed columns), derived **before** drop.

## v2 — `apply_transformations` (25 min)

Build it one step at a time, running after each block:

```python
def apply_transformations(df, t):
    # 1. null literals in string columns
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string" and not field.name.startswith("_"):
            df = df.withColumn(field.name, F.when(F.trim(F.col(field.name)).isin(t.get("null_literals", [])), None).otherwise(F.col(field.name)))
    # 2. trim
    for column in t.get("trim", []):
        if column in df.columns:
            df = df.withColumn(column, F.trim(F.col(column)))
    # 3. rename
    for old, new in t.get("rename", {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    # 4. cast
    for column, spark_type in t.get("cast", {}).items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(spark_type))
    # 5. parse json
    for column, ddl in t.get("parse_json", {}).items():
        if column in df.columns:
            df = df.withColumn(column, F.from_json(F.col(column), ddl))
    # 6. derived
    for column, expression in t.get("derived", {}).items():
        df = df.withColumn(column, F.expr(expression))
    # 7. filter, drop
    if t.get("filter"):
        df = df.filter(t["filter"])
    if t.get("drop"):
        df = df.drop(*[c for c in t["drop"] if c in df.columns])
    return df

silver_customers = apply_transformations(spark.table("retaildataplatform.bronze.customers"), transformations)
```

`t.get("trim", [])` — "give me this key, or an empty list if it is missing" — so every
section is optional. `df.drop(*list)` — the `*` unpacks a list into arguments.

Exercise (10 min): write the `sales_orders` dictionary: `rename` `_id`→`source_document_id`,
casts, `parse_json` for the three JSON columns (schema strings from Class 8 homework),
derived `order_ts`, `order_date`, `line_item_count`, `has_promotion`, drop `order_datetime`.

## Mini-lesson: ANSI mode and `try_cast` (15 min)

Tell the real story: locally the pipeline passed; on Databricks `b2s` crashed with
`CAST_INVALID_INPUT: '1.564627663E9' cannot be cast to BIGINT`. Databricks runs SQL in
ANSI mode — a bad cast is an *error*, not a NULL.

```sql
SELECT CAST('1.564627663E9' AS BIGINT);                      -- error on Databricks
SELECT try_cast('1.564627663E9' AS BIGINT);                  -- NULL
SELECT try_cast(try_cast('1.564627663E9' AS DOUBLE) AS BIGINT); -- 1564627663
```

Replace step 4 with the reference implementation:

```python
INTEGRAL = {"tinyint", "smallint", "int", "integer", "bigint", "long", "short", "byte"}

def tolerant_cast(column, spark_type):
    target = spark_type.strip().lower()
    if target in INTEGRAL:
        return F.expr(f"try_cast(try_cast(`{column}` AS DOUBLE) AS {target})")
    return F.expr(f"try_cast(`{column}` AS {spark_type})")
```

Principle: **a transformation never crashes on one bad value; it produces NULL and a
quality rule (Class 18) decides whether NULL is acceptable.**

## Homework

1. Finish the `sales` dictionary: rename, cast, the five `regexp_extract` derived columns, `sale_id` hash, drop `product_json`. Verify `total_amount = unit_price * quantity` for every row.
2. Move the three dictionaries into `src/config/silver.json` under `"entities": [{"source_table": ..., "target_table": ..., "transformations": {...}}]` and load them with `json.load`.
3. Write down the seven step order and one reason for each position.

## Common problems

* Backslashes: in JSON `'\\\\s+'` becomes the Python string `'\\s+'` becomes the SQL regex `\s+`. Show the three layers once, slowly.
* `split_part` exists on Databricks (Spark 3.4+); if a student's local Spark is older, use `split(...)[0]`.
* A derived column that references a column renamed in the same dictionary must use the *new* name.
