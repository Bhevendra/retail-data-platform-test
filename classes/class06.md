# Class 6 — Raw → Bronze: Delta tables and audit columns

## Objectives

* Explain what Delta Lake adds to a folder of files (transactions, history, schema).
* Read the landed files for one source and one `load_date` back into a DataFrame.
* Add audit columns (`_load_date`, `_ingested_at`, `_run_id`, `_source_system`, `_source_file`).
* Write the result as a Unity Catalog Delta table and inspect its history.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: three folders in the volume; what Bronze adds |
| 10–30 | Mini-lesson: Delta Lake in five commands |
| 30–50 | v1: read one landed source, add audit columns by hand |
| 50–70 | v2: `saveAsTable`, DESCRIBE HISTORY, time travel |
| 70–85 | v3: wrap as `read_landed_raw` + `with_audit_columns` functions |
| 85–95 | Homework |

## Mini-lesson: Delta Lake (20 min)

Create a tiny table and show the five things students must know:

```sql
CREATE OR REPLACE TABLE retaildataplatform.bronze.demo AS SELECT 1 AS id, 'a' AS v;
INSERT INTO retaildataplatform.bronze.demo VALUES (2, 'b');
UPDATE retaildataplatform.bronze.demo SET v = 'B' WHERE id = 2;
DESCRIBE HISTORY retaildataplatform.bronze.demo;                 -- every change is a version
SELECT * FROM retaildataplatform.bronze.demo VERSION AS OF 1;    -- time travel
```

Then `DESCRIBE DETAIL` to show it is still files in storage plus a `_delta_log`. Key
sentence: **"Delta is Parquet files plus a diary of what happened to them."**

## v1 — read landed files and add audit columns (20 min)

```python
from pyspark.sql import functions as F
import uuid

run_date = "2026-09-03"
run_id = str(uuid.uuid4())
source_name = "sqlserver_customers"
path = f"/Volumes/retaildataplatform/bronze/raw_data/{source_name}/load_date={run_date}"

raw = (spark.read.option("header", "true").option("inferSchema", "true")
       .option("escape", '"').option("multiLine", "true").csv(path))

bronze = (raw
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_load_date", F.lit(run_date).cast("date"))
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_source_system", F.lit(source_name)))

display(bronze.select("customer_id", "_source_file", "_load_date", "_ingested_at", "_run_id").limit(5))
```

Explain each audit column with a question it answers:

| Column | Question it answers later |
| --- | --- |
| `_load_date` | Which day's extract is this row from? (re-runs replace by this) |
| `_ingested_at` | When exactly was it written? |
| `_run_id` | Which run produced it? (joins to run logs in Class 17) |
| `_source_system` | Which configured source? |
| `_source_file` | Which raw file? (`_metadata.file_path` is free lineage from Spark) |

`uuid.uuid4()` — a random unique id; run the cell twice to show it changes.

## v2 — write a Delta table (20 min)

```python
bronze.write.format("delta").mode("overwrite").saveAsTable("retaildataplatform.bronze.customers")
```

```sql
SELECT count(*), min(_ingested_at), max(_ingested_at) FROM retaildataplatform.bronze.customers;
DESCRIBE HISTORY retaildataplatform.bronze.customers;
DESCRIBE EXTENDED retaildataplatform.bronze.customers;
```

Run the write cell a second time and look at the history: version 1 replaced version 0.
Ask: "what if yesterday's data was in the table?" — overwrite would delete it. That is
tomorrow's problem (Class 7) and the reason Bronze must be **append-by-day, replace-by-day**.

Repeat v1 + v2 for `s3_sales` (`spark.read.parquet`) and `cosmos_sales_orders`
(`spark.read.json`) — students do it themselves, 10 min.

## v3 — functions (15 min)

```python
def read_landed_raw(source_name, file_format, run_date):
    path = f"/Volumes/retaildataplatform/bronze/raw_data/{source_name}/load_date={run_date}"
    print("reading raw from:", path)
    reader = spark.read.format(file_format)
    if file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true").option("escape", '"').option("multiLine", "true")
    df = reader.load(path).withColumn("_source_file", F.col("_metadata.file_path"))
    return df, path

def with_audit_columns(df, run_date, run_id, source_name):
    return (df.withColumn("_load_date", F.lit(run_date).cast("date"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_run_id", F.lit(run_id))
              .withColumn("_source_system", F.lit(source_name)))

raw, path = read_landed_raw("sqlserver_customers", "csv", "2026-09-03")
bronze = with_audit_columns(raw, "2026-09-03", str(uuid.uuid4()), "sqlserver_customers")
```

Point out: a function can return two things (`return df, path`) and the caller unpacks
them. Compare with `common_utils/bronze.py::with_audit_columns` — same idea, plus two
legacy columns (`last_update_ts`, `file_path`) kept for early consumers; discuss why you
keep old column names when someone already depends on them.

## Homework

1. What happens if the `load_date` folder does not exist? Run it, read the error, then add an `if` using `dbutils.fs.ls` inside `try/except` that prints a friendly message (preview of Class 17).
2. Query Bronze `customers`: how many distinct `_source_file` values? Why more than one?
3. Write the three Bronze tables and take a screenshot of `DESCRIBE HISTORY` for each.

## Common problems

* `_metadata.file_path` only exists on file-based reads; on a table read it does not — fine, we only use it on raw reads.
* `saveAsTable` with an existing table of a different schema → `AnalysisException`; use `mode("overwrite")` in class, and learn the schema rules in Class 7.
* CSV without `multiLine`/`escape` splits quoted names containing newlines — keep the four options together always.
