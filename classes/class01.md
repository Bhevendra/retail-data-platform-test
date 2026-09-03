# Class 1 — The project, the platform, the plan

## Objectives

By the end of this class every student can:

* describe in their own words what the finished platform does and why each layer exists;
* navigate Databricks Free Edition: workspace, notebooks, Serverless compute, Catalog Explorer;
* create a catalog, a schema and a volume in Unity Catalog and upload a file to the volume;
* run a notebook with a widget (parameter) and explain why parameters matter.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–15 | The story: a retailer with three systems and one question |
| 15–35 | Mini-lesson: medallion architecture (raw → bronze → silver → gold) |
| 35–55 | Platform tour: Databricks Free Edition, Serverless, Unity Catalog |
| 55–80 | Hands-on: catalog, schema, volume, upload CSVs, first notebook, widgets |
| 80–95 | The course map, how we will work, homework |

## 1. The story (15 min)

Draw on the whiteboard three boxes and one person.

* **Web shop** stores orders as JSON documents in **Cosmos DB** (every order has a list of products, promotions, a click trail).
* **Stores** export their point-of-sale receipts nightly to **Amazon S3** as a Parquet file.
* **CRM** keeps the customer master in **Azure SQL Server**.
* **The person** is a BI analyst who asks: *"What was net revenue by brand and loyalty segment last month, online vs in store?"*

Ask the class: how would you answer that today? Let them realise you would need three logins, three tools, three formats, and that tomorrow the numbers would already be different. **That gap is the whole reason a data platform exists.**

Show the finished dashboard query (from `docs/consumers.md`):

```sql
SELECT year_month, brand, MEASURE(net_revenue) AS net_revenue, MEASURE(orders) AS orders
FROM retaildataplatform.gold.mv_web_sales GROUP BY 1, 2 ORDER BY 1, 2;
```

"In 26 classes you will have built everything that makes this one query possible."

## 2. Mini-lesson: medallion architecture (20 min)

Explain with the kitchen analogy: raw ingredients delivered (raw), unpacked and labelled in the fridge (bronze), washed and chopped (silver), plated dishes on the menu (gold).

| Layer | What we do | What we never do |
| --- | --- | --- |
| Raw (volume) | Keep the original file exactly as received, one folder per day | Change it |
| Bronze | Load into a table, add "when/where did this come from" columns, check basic rules | Rename or reshape business columns |
| Silver | Clean, type, rename, remove duplicates, keep history (SCD) | Aggregate or join for reporting |
| Gold | Star schema (facts + dimensions), views, metrics for BI/AI | Store anything you cannot rebuild from Silver |

Key sentence to repeat all course long: **"Every layer can be rebuilt from the layer below it."**

Show the reference repo's README diagram and read it top to bottom, one arrow at a time.

## 3. Platform tour (20 min)

Log in to Databricks Free Edition and walk through, students following on their own accounts:

1. **Workspace** — folders, notebooks, files. Create a folder `bootcamp`.
2. **Compute** — Serverless is the only option; explain "no cluster to manage, but some things are not allowed: no JVM tricks, no caching, Python libraries via `%pip`". (They will meet all three restrictions later as real errors — tell them so.)
3. **Catalog Explorer** — the three-level namespace: `catalog.schema.table`. Volumes live inside schemas and hold *files*, tables hold *rows*.
4. **SQL editor** vs **notebook** — same SQL, different place.
5. **Secrets** exist (we will use them in Class 5); show that the CLI is needed and park it.

## 4. Hands-on (25 min)

### 4.1 Create the namespace (SQL cell)

```sql
CREATE CATALOG IF NOT EXISTS retaildataplatform;
CREATE SCHEMA  IF NOT EXISTS retaildataplatform.bronze;
CREATE VOLUME  IF NOT EXISTS retaildataplatform.bronze.raw_data;
```

Checkpoint: everyone sees the volume in Catalog Explorer under `bronze`.

### 4.2 Upload the practice files

Upload `customers.csv`, `sales.csv`, `sales_orders.csv` into the volume through the UI,
into a folder `practice/`. Then in a Python cell:

```python
files = dbutils.fs.ls("/Volumes/retaildataplatform/bronze/raw_data/practice/")
for f in files:
    print(f.name, f.size)
```

Explain `dbutils.fs.ls` returns a list, `for` loops over it — nothing new, just a new list.

### 4.3 First look at the data

```python
df = spark.read.option("header", "true").csv("/Volumes/retaildataplatform/bronze/raw_data/practice/customers.csv")
df.printSchema()
display(df.limit(5))
print("rows:", df.count())
```

Ask: what type is `customer_id`? (string — because we did not ask Spark to infer). Add `.option("inferSchema", "true")` and rerun. Explain the cost: Spark reads the file twice.

### 4.4 Parameters with widgets

Version 1 — hard-coded:

```python
path = "/Volumes/retaildataplatform/bronze/raw_data/practice/customers.csv"
df = spark.read.option("header", "true").csv(path)
print(df.count())
```

Version 2 — the file name becomes a parameter:

```python
dbutils.widgets.text("file_name", "customers.csv")
file_name = dbutils.widgets.get("file_name")
path = "/Volumes/retaildataplatform/bronze/raw_data/practice/" + file_name
print("reading:", path)
df = spark.read.option("header", "true").csv(path)
print("rows:", df.count())
```

Change the widget at the top of the notebook to `sales.csv` and rerun the cell. This is the first time they see "same code, different input" — the theme of the entire course. Mention that a scheduled job will fill the same widget automatically (Class 20).

## 5. The course map and working agreements (15 min)

* Show `classes/README.md` and read the phase table.
* Working agreements: one notebook per class in `bootcamp/classNN`, everything committed to git from Class 23 onwards, and *"we never delete an old version, we make a new cell below it"* — students should be able to scroll up and see v1 → v2 → v3.
* Explain the ladder (hard-coded → function → prints → config → logging → module) and tell them they will climb it dozens of times until it is boring.

## Homework

1. Repeat 4.1–4.4 from scratch without looking.
2. Read `docs/architecture.md` "Principles" section; write one sentence per principle in your own words.
3. Using only what they know: count rows per `state` in `customers.csv` and sort descending.

## Common problems

* "Catalog already exists / permission denied" — in Free Edition the user owns the workspace; usually a typo in the catalog name.
* Uploaded file lands outside `practice/` — show how to move it in Catalog Explorer or `dbutils.fs.mv`.
* Widget not updating — widgets are read when the cell runs; rerun the cell after changing the value.
