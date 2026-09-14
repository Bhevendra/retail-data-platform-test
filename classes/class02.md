# Class 2 — S3 ingestion and raw → Bronze Delta tables

> Taught as delivered (recording 02:27). Still fully hard-coded, still one notebook per
> source — by the end of this class there are **six** near-identical notebooks, and that
> duplication is the pain Class 3 removes.

## Objectives

By the end of this class every student can:

* read a Parquet file from an S3 bucket with Spark and land it in the raw volume;
* explain what Parquet is and why the raw copy keeps the source's own format;
* read landed files back and write them as a **Delta table** in the Bronze schema;
* add the two audit columns `last_update_ts` and `file_path` and say what question each
  one answers;
* describe the difference between the raw volume (files) and Bronze (tables).

## Time plan (120 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap of Class 1; today's target on the whiteboard |
| 10–25 | Mini-lesson: object storage, Parquet, and `s3a://` |
| 25–50 | Live code: S3 → volume |
| 50–65 | Mini-lesson: Delta tables, and why Bronze is a table at all |
| 65–80 | Mini-lesson: audit columns and lineage |
| 80–110 | Live code: three `ds2b` notebooks (S3, Cosmos, SQL Server) |
| 110–120 | Compare the three, recap, homework |

## 1. Mini-lesson: object storage and Parquet (15 min)

* **S3** is object storage: a bucket holds objects addressed by a key
  (`retail/sales.parquet`). It is not a database and it is not a filesystem, even though
  the path looks like one.
* **Parquet** is columnar and compressed: it stores column by column instead of row by
  row, so reading three columns out of thirty reads roughly a tenth of the bytes. It also
  carries its own schema, so unlike CSV there is nothing to infer.
* `s3a://` is the protocol Spark uses to talk to S3. Credentials can be embedded in the
  URL — which is exactly what we do today and exactly what we will stop doing.

Say it out loud: **these keys are going into a notebook today and a secret scope later.**
Keys in notebooks get committed, screenshotted and shared. Treat any key that has ever
appeared in a notebook as compromised and rotate it.

## 2. Live code: S3 → volume (25 min)

Notebook: `s3-ingestion`

```python
from datetime import date

aws_access_key_id = "<access-key-id>"
aws_secret_access_key = "<secret-access-key>"

source_path = (
    f"s3a://{aws_access_key_id}:{aws_secret_access_key}"
    f"@bucket-rivadata/retail/sales.parquet"
)

df = spark.read.parquet(source_path)
display(df)
print("rows:", df.count())
```

Look at the data together. `product` is a JSON string sitting inside a Parquet column —
a good moment to say that "structured format" and "clean data" are different things, and
that this column is another entry on Silver's to-do list.

```python
run_date = date.today().isoformat()

target_path = f"/Volumes/retaildataplatform/bronze/raw_data/s3_sales/load_date={run_date}"

(df.write
   .mode("overwrite")
   .parquet(target_path))

for f in dbutils.fs.ls(target_path):
    print(f.name, f.size)
```

Why Parquet here and CSV for SQL Server? **The raw copy keeps the format the source gave
us.** S3 sent Parquet, so we keep Parquet. There is no conversion to argue about later.

> An alternative you can show in one line: `dbutils.fs.cp(source_path, target_path)`
> copies the bytes without Spark reading them at all. Ask the class which one they would
> pick and why. (`cp` is a true byte-for-byte copy; `spark.read` + `write` re-encodes the
> file and re-partitions it.)

Checkpoint: three source folders now exist under `raw_data/`.

## 3. Mini-lesson: what Bronze is, and why it is a table (15 min)

So far everything is **files in a volume**. Files are good evidence and bad workplaces:
no `SELECT`, no schema enforcement, no history, no time travel.

Bronze is the same data as a **table** — specifically a **Delta table**:

* Delta = Parquet files + a transaction log.
* The log gives atomic writes (a reader never sees half a write), schema tracking, and
  time travel (`DESCRIBE HISTORY`, `VERSION AS OF`).
* It is queryable by anyone with SQL, and it appears in Catalog Explorer with its schema.

So the shape of the platform is:

```
raw volume (files, as received)  →  bronze tables (same data, queryable, + audit columns)
```

Still no cleaning. Bronze is "the raw data, in a table, with a note about where it came
from".

## 4. Mini-lesson: audit columns (15 min)

Two columns get added to every Bronze table:

| Column | Question it answers |
| --- | --- |
| `last_update_ts` | When did this row land in Bronze? (freshness, incremental loads, debugging) |
| `file_path` | Which file did this row come from? (lineage, "which extract broke?") |

`file_path` comes from a Spark trick: when Spark reads files it exposes a hidden
`_metadata` column, so `F.col("_metadata.file_path")` gives the exact file each row came
from. Note that this only works when reading **files** — it is why we add the columns in
this notebook (reading landed files) rather than in the ingestion notebook (where the
Cosmos data came from an API call, not a file).

Ask the class: on the day a customer complains that their address is wrong, which of these
two columns tells you which nightly extract to look at? That is why they exist.

## 5. Live code: raw → Bronze, three times (30 min)

Notebook: `ds2b_s3` ("data source to Bronze").

```python
import pyspark.sql.functions as F

print("Defining raw path")
raw_path = "/Volumes/retaildataplatform/bronze/raw_data/s3_sales/load_date=2026-09-14/"
print(f"raw path given is {raw_path}")

print("reading data")
df = (spark.read
      .format("parquet")
      .load(raw_path))

df.show()

print("adding audit columns")
df = (df.withColumn("last_update_ts", F.current_timestamp())
        .withColumn("file_path", F.col("_metadata.file_path")))

target_table = "retaildataplatform.bronze.s3_sales"
print(f"target table is {target_table}")

print("writing data")
(df.write.format("delta")
   .mode("overwrite")
   .saveAsTable(target_table))

print("Data written successfully at", target_table)
```

Point at the `print` statements deliberately: *"this is how you find out where a notebook
died at 3am."* Then plant the flag for next class: prints have no timestamp, no severity,
no way to be switched off, and they are copied into every notebook by hand. Class 3
replaces all of them in fifteen minutes.

Now the same notebook twice more, changing only three things each time:

| Notebook | `raw_path` folder | `format` | `target_table` |
| --- | --- | --- | --- |
| `ds2b_s3` | `s3_sales` | `parquet` | `retaildataplatform.bronze.s3_sales` |
| `ds2b_cosmosdb` | `cosmosdb_sales_orders` | `json` | `retaildataplatform.bronze.cosmosdb_sales_orders` |
| `ds2b_sqlserver` | `sqlserver_customers` | `csv` (+ `header`, `inferSchema`) | `retaildataplatform.bronze.sqlserver_customers` |

The SQL Server one needs the two CSV options, because CSV carries no types:

```python
df = (spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(raw_path))
```

Have them copy-paste and edit rather than retype — the copy-pasting **is** the lesson.
When the third notebook is done, ask: *"if we want to rename `last_update_ts`, how many
files do we open?"* Three. And after the fourth source? Four. Write **"1 change = 3 edits"**
on the board and leave it there for next class.

Checkpoint: Catalog Explorer shows three tables under `retaildataplatform.bronze`, each
with the two audit columns at the end.

```sql
SELECT * FROM retaildataplatform.bronze.s3_sales LIMIT 10;
DESCRIBE TABLE EXTENDED retaildataplatform.bronze.cosmosdb_sales_orders;
DESCRIBE HISTORY retaildataplatform.bronze.sqlserver_customers;
```

`DESCRIBE HISTORY` is worth thirty seconds on its own: that is the Delta log, and it is
why we chose a Delta table over a pile of Parquet files.

## 6. Recap (10 min)

| Source | Format | Ingestion method | Raw volume path | Bronze table |
| --- | --- | --- | --- | --- |
| Amazon S3 | Parquet | Spark (`s3a://`) | `raw_data/s3_sales/load_date=…` | `bronze.s3_sales` |
| Cosmos DB | JSON | PyMongo | `raw_data/cosmosdb_sales_orders/load_date=…` | `bronze.cosmosdb_sales_orders` |
| SQL Server | CSV | Spark JDBC | `raw_data/sqlserver_customers/load_date=…` | `bronze.sqlserver_customers` |

All three sources are in. Six notebooks, almost identical. Next class we stop copying.

## Homework

1. Run every `ds2b` notebook a second time, then run `DESCRIBE HISTORY` on each table.
   How many versions do you have, and what does `operation` say? What did `overwrite` do
   to yesterday's rows?
2. `SELECT count(DISTINCT file_path) FROM retaildataplatform.bronze.s3_sales` — what number
   do you expect before you run it, and why?
3. Land the S3 file again into **today's** folder, then point `ds2b_s3` at the parent
   folder `raw_data/s3_sales/` instead of one date folder. What extra column appears, and
   where did it come from?
4. Write down, for each of `last_update_ts` and `file_path`, one real incident where that
   column would save you an hour.
5. Count the lines that are **identical** across your three `ds2b` notebooks. Bring the
   number to next class.
6. **Stretch** — the Cosmos Bronze table has one column of JSON text. Using
   `F.get_json_object(F.col("json_data"), "$.order_number")`, add a column with the order
   number. Do not save it; just prove to yourself the data is in there.

## Common problems

* `Path does not exist` in `ds2b` — the `load_date=` folder in the raw path is
  hard-coded to a date that has no data. This trips everyone at least once, which is
  precisely why `run_date` becomes a parameter later.
* `_metadata.file_path` fails after `createDataFrame` — `_metadata` exists only when
  Spark read actual files.
* CSV Bronze has every column as `string` — `inferSchema` was forgotten. Show the
  difference; then mention that real typing belongs in Silver anyway.
* `Table already exists` — `saveAsTable` without `mode("overwrite")`.
* Someone edits the S3 keys into a notebook they then share. Repeat the rotation rule.
