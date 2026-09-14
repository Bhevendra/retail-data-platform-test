# Class 1 — Ingestion from multiple sources (hard-coded)

> This is the class as actually taught (recording 02:17). Everything is typed straight
> into the notebook: no functions, no config, no logging. That is deliberate — the next
> classes each earn one of those by removing a specific pain.

## Objectives

By the end of this class every student can:

* name the three source systems, what kind of data each holds, and how we connect to each;
* explain why raw data lands in a volume **as-is** before anything else happens;
* create a catalog, a schema and a volume in Unity Catalog;
* read a SQL Server table with JDBC and land it as CSV under `load_date=YYYY-MM-DD`;
* read a Cosmos DB collection with PyMongo and land it as JSON in the same structure.

## Time plan (120 min)

| Min | Segment |
| --- | --- |
| 0–20 | The project: three sources, one question |
| 20–35 | Mini-lesson: why raw/Bronze first, and the medallion picture |
| 35–50 | Unity Catalog: catalog → schema → volume, the three-level namespace |
| 50–60 | Mini-lesson: JDBC |
| 60–85 | Live code: SQL Server → volume (CSV) |
| 85–95 | Mini-lesson: document databases, BSON, why JSON strings |
| 95–115 | Live code: Cosmos DB → volume (JSON) |
| 115–120 | Recap, source comparison, homework |

## 1. The project (20 min)

Three boxes on the whiteboard, one person:

| | Azure SQL Server | Cosmos DB | Amazon S3 |
| --- | --- | --- | --- |
| Type | Relational | NoSQL / document (Mongo API) | File storage |
| Data | Structured | Semi-structured | Parquet files |
| Holds | CRM customers | Web-shop sales orders | Point-of-sale receipts |
| Source object | Table | Collection | Object in a bucket |
| Data unit | Rows | Documents | Files |
| We connect with | JDBC | PyMongo | Spark over `s3a://` |
| Lands as | CSV | JSON | Parquet |

The person is a BI analyst who asks: *"What was net revenue by brand and loyalty segment
last month, online vs in store?"* Today that question needs three logins, three tools and
three formats. **Closing that gap is the whole reason a data platform exists.**

Tell them the destination up front: those three feeds become clean Silver tables and then
a Gold star schema that answers the question in one SQL query.

## 2. Mini-lesson: why raw/Bronze first (15 min)

The rule: **land the data exactly as it arrived, then transform copies of it.**

Why:

* **Traceability** — you can always prove what the source actually sent.
* **Reprocessing** — if tomorrow you find the logic was wrong, you fix the code and replay
  the landed files; you do not go back and ask the source for last month again.
* **Decoupling** — the source may be available for ten minutes a night; your
  transformations can run any time against the landed copy.
* **One landing area** — every source, one volume, organised by source and load date.

What we never do in this layer: clean, rename, deduplicate, join, or "fix" anything.

The kitchen analogy, which comes back all course long: raw ingredients delivered (raw),
unpacked and labelled in the fridge (Bronze), washed and chopped (Silver), plated dishes
on the menu (Gold). And the sentence to repeat: **every layer can be rebuilt from the
layer below it.**

## 3. Unity Catalog: catalog, schema, volume (15 min)

```sql
CREATE CATALOG IF NOT EXISTS retaildataplatform;
CREATE SCHEMA  IF NOT EXISTS retaildataplatform.bronze;
CREATE VOLUME  IF NOT EXISTS retaildataplatform.bronze.raw_data;
```

Draw the three-level namespace and say it out loud every time from now on:

```
catalog . schema . object
retaildataplatform . bronze . raw_data
```

* A **table** holds rows. A **volume** holds files. Both live inside a schema.
* The volume is reachable as an ordinary path:
  `/Volumes/retaildataplatform/bronze/raw_data/`
* Show the same object in Catalog Explorer (Catalog → retaildataplatform → bronze →
  Volumes → raw_data) so they see that the UI and the SQL are the same thing.

Checkpoint: everyone sees `raw_data` in Catalog Explorer and
`dbutils.fs.ls("/Volumes/retaildataplatform/bronze/raw_data/")` runs without error (an
empty list is a success, not a failure).

## 4. Mini-lesson: JDBC (10 min)

JDBC is "a phone number plus a driver". The URL says where to call and how; the driver is
the software that speaks SQL Server. Write it on the board and label every part:

```
jdbc:sqlserver://<server>.database.windows.net:1433;databaseName=<db>;encrypt=true;trustServerCertificate=true;loginTimeout=90
     ^protocol   ^host                           ^port ^which database ^TLS on  ^accept the server certificate  ^wait up to 90s
```

`trustServerCertificate=true` and `loginTimeout=90` are there because Serverless failed
without them. A real production lesson, not theory.

## 5. Live code: SQL Server → volume (25 min)

Notebook: `sql-server-ingestion`

```python
from datetime import date

url = "jdbc:sqlserver://<server>.database.windows.net:1433;databaseName=<db>;encrypt=true;trustServerCertificate=true;loginTimeout=90"

user = "<username>"          # hard-coded today; secret scope in a later class
password = "<password>"
dbtable = "retail.customers"

df = (spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .load())

display(df)
print("rows:", df.count())
```

Stop and look at the data together: `customer_id` is already an integer (JDBC carries
types with it, unlike CSV), `valid_from` is a big number (an epoch — a later class),
`NULL` appears as text in some columns, postcodes look like `46506.0`. Do not fix
anything. Name the problems and write them on the board as **Silver's to-do list**.

Say it plainly: **a password typed into a notebook is wrong.** We do it today so there is
only one new thing at a time; a later class replaces it with a secret scope.

### Land it

```python
run_date = date.today().isoformat()          # '2026-09-14'

target_path = f"/Volumes/retaildataplatform/bronze/raw_data/sqlserver_customers/load_date={run_date}"

(df.write
   .mode("overwrite")
   .option("header", "true")
   .option("quoteAll", "true")
   .option("escape", '"')
   .csv(target_path))

for f in dbutils.fs.ls(target_path):
    print(f.name, f.size)
```

Explain every line:

* `mode("overwrite")` — running twice on the same day replaces that day's folder instead
  of doubling it. Write the word **idempotent** on the board; it returns many times.
* `quoteAll` / `escape` — customer names look like `SMITH,  SHIRLEY`. Without quoting, the
  comma inside the value would break the CSV.
* `load_date=` in the folder name — Spark reads `key=value` folders as a column, and more
  importantly a human can tell what a folder holds without opening it.
* Spark writes `part-00000-*.csv`, not one tidy file. That is normal: a folder *is* the
  dataset.

Checkpoint: the folder exists in Catalog Explorer with part files inside.

## 6. Mini-lesson: document databases (10 min)

* Cosmos DB with the Mongo API stores **documents**, not rows. Each sales order is one
  JSON document with nested arrays inside it: ordered products, clicked items, promotions.
* Two documents in the same collection need not have the same fields. There is no schema.
* Mongo returns **BSON**, which has types JSON does not (`ObjectId`, dates). That is why
  we serialise with `bson.json_util` rather than plain `json.dumps` — `json_util` knows
  how to write those types as valid JSON.
* We keep each document whole, as one JSON string per row. **No flattening today.** The
  nested structure is the source's truth; unpacking it is a Silver job.

## 7. Live code: Cosmos DB → volume (20 min)

Notebook: `cosmosdb-ingestion`

```python
from pymongo import MongoClient
from bson import json_util
from datetime import date

connection_string = "<cosmos-connection-string>"
database_name = "retail"
collection_name = "sales_orders"

client = MongoClient(connection_string)
db = client[database_name]
collection = db[collection_name]

documents = list(collection.find({}))
print("documents:", len(documents))
```

`collection.find({})` means "find everything" — the empty dictionary is the filter. Show
one document with `print(documents[0])` and let them see the nesting.

```python
json_rows = []

for document in documents:
    json_string = json_util.dumps(document)
    json_rows.append((json_string,))

df = spark.createDataFrame(json_rows, ["json_data"])
display(df)
```

Three things worth pausing on, because they are what students get wrong later:

1. `json_rows` is a list of **tuples**. The trailing comma in `(json_string,)` is what
   makes it a one-element tuple instead of a string — a classic typo.
2. `createDataFrame` turns Python objects into a Spark DataFrame. Until this line the data
   sat in the driver's memory. Fine for 4,500 documents; not fine for 4.5 million. Mention
   it, do not solve it today.
3. The DataFrame has **one column**, `json_data`. We are storing text that happens to be
   JSON. Silver will parse it.

```python
run_date = date.today().isoformat()

target_path = f"/Volumes/retaildataplatform/bronze/raw_data/cosmosdb_sales_orders/load_date={run_date}"

(df.write
   .format("json")
   .mode("overwrite")
   .save(target_path))
```

Checkpoint: both folders now sit side by side under `raw_data/`, each with its own
`load_date=` folder.

## 8. Recap (5 min)

```
/Volumes/retaildataplatform/bronze/raw_data/
├── sqlserver_customers/load_date=2026-09-14/part-00000-*.csv
└── cosmosdb_sales_orders/load_date=2026-09-14/part-00000-*.json
```

Same structure, different format, no transformations. Next class: the third source (S3),
and turning all of this into Bronze **tables**.

## Homework

1. Land `retail.customers` again into **yesterday's** folder
   (`date.today() - timedelta(days=1)`), then list both folders and confirm each has its
   own part files.
2. Read your landed CSV back with
   `spark.read.option("header", "true").option("inferSchema", "true").csv(target_path)`
   and prove `back.count()` equals the count you printed from SQL Server. Why is proving
   the round trip worth 30 seconds of your time?
3. Count customers per `state` from the SQL Server DataFrame, sorted descending. Which
   three states dominate?
4. Print the first Cosmos document and write down, in your own words, the three arrays it
   contains and what one element of each represents.
5. In one paragraph: what does "land raw first" protect you from? Give a concrete example.
6. **Stretch** — land only California customers by replacing `dbtable` with
   `.option("query", "SELECT * FROM retail.customers WHERE state = 'CA'")`. How many rows?

## Common problems

* `[FAILED_JDBC.CONNECTION] ... SQLSTATE: HV000` — Databricks hides the real cause. Teach
  the method, not the fix: (1) is the server reachable at all
  (`socket.create_connection((host, 1433), timeout=10)`)? (2) are the login, database and
  table correct? (3) if 1 and 2 are fine it is driver options — add
  `trustServerCertificate=true;loginTimeout=90`.
* A wrong `databaseName` produces the *same* generic error as a wrong password. That is
  exactly why the three-step method exists.
* `UserWarning: You appear to be connected to a CosmosDB cluster` — harmless; PyMongo
  noticing it is not talking to real MongoDB.
* Forgetting `mode("overwrite")` → "path already exists" on the second run.
* Someone writes to the root of `raw_data/` instead of `<source>/load_date=…`. Insist on
  the structure from day one; the whole platform depends on it.
