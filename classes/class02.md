# Class 2 — SQL Server → volume (hard-coded)

## Objectives

* Read a table from Azure SQL Server into a DataFrame with the JDBC reader.
* Write that DataFrame as CSV into the raw volume, in a folder named by date.
* Explain the "land raw first" rule and why the folder is named `load_date=YYYY-MM-DD`.
* Read a JDBC error message and know the three things that can be wrong (network, login, object).

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap + today's target on the whiteboard |
| 10–25 | Mini-lesson: JDBC and connection strings |
| 25–55 | v1: hard-coded read from SQL Server, explore |
| 55–75 | v2: land as CSV into the volume; date folder |
| 75–90 | Mini-lesson: diagnosing connection failures (the real error we hit) |
| 90–95 | Homework |

## Mini-lesson: JDBC (15 min)

JDBC is "a phone number plus a driver". The URL tells Spark where to call and how; the
driver is the piece of software that speaks SQL Server. Write the URL on the board and
label every part:

```
jdbc:sqlserver://rivadata.database.windows.net:1433;databaseName=batch2;encrypt=true;trustServerCertificate=true;loginTimeout=90
      ^protocol   ^host                          ^port ^which database   ^TLS on   ^accept the server's certificate  ^wait up to 90 s
```

Tell them `trustServerCertificate=true` and `loginTimeout=90` exist because Serverless
failed without them — this is a real production lesson, not theory.

## v1 — read the table (30 min)

```python
url = "jdbc:sqlserver://rivadata.database.windows.net:1433;databaseName=batch2;encrypt=true;trustServerCertificate=true;loginTimeout=90"
user = "PUT_USERNAME_HERE"       # hard-coded today, secret scope in Class 5
password = "PUT_PASSWORD_HERE"

df = (spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", "retail.customers")
      .option("user", user)
      .option("password", password)
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .load())

print("rows:", df.count())
df.printSchema()
display(df.limit(10))
```

Stop and discuss what they see: `customer_id` is already an integer (JDBC brings types
with it, unlike CSV), `valid_from` is a big number (an epoch — Class 8 explains), `NULL`
text in some columns.

Exercise (10 min, known skills only): how many customers per `state`? How many have
`valid_to` NULL? (`filter(col("valid_to").isNull()).count()`).

Say explicitly: **hard-coding a password in a notebook is wrong**, we do it today only so
the class has one new thing at a time; Class 5 fixes it.

## v2 — land into the raw volume (20 min)

Why not go straight to a table? Because the file is the evidence. If the source changes
tomorrow, the file from today still exists and we can rebuild everything from it.

```python
from datetime import date

run_date = date.today().isoformat()          # '2026-09-03'
target = "/Volumes/retaildataplatform/bronze/raw_data/sqlserver_customers/load_date=" + run_date
print("landing to:", target)

(df.write.mode("overwrite")
   .option("header", "true")
   .option("quoteAll", "true")
   .option("escape", '"')
   .csv(target))

for f in dbutils.fs.ls(target):
    print(f.name, f.size)
```

Explain each line:

* `mode("overwrite")` — running twice on the same day replaces the folder (idempotent by date, the word comes back in Class 7).
* `quoteAll` / `escape` — names like `SMITH,  SHIRLEY` contain commas; without quoting the CSV would break.
* The folder name `load_date=...` — Spark understands `key=value` folder names as a column when reading a parent folder; more importantly humans understand it.

Checkpoint: everyone has a folder with `part-*.csv` files in Catalog Explorer.

Read it back to prove the round trip:

```python
back = spark.read.option("header", "true").option("inferSchema", "true").csv(target)
print(back.count() == df.count())
```

## Mini-lesson: when the connection fails (15 min)

Show the real error text students may see: `[FAILED_JDBC.CONNECTION] ... SQLSTATE: HV000`.
Databricks hides the real cause. Teach the three-step diagnosis exactly as it happened
in the project:

1. **Network** — can we reach the server at all?
   ```python
   import socket
   socket.create_connection(("rivadata.database.windows.net", 1433), timeout=10).close()
   print("TCP reachable")
   ```
   If this fails: firewall on the Azure SQL server.
2. **Login / database / table** — try a pure-Python client that prints the real server message (`%pip install python-tds pyOpenSSL certifi`, then `pytds.connect(...)`).
3. **Driver options** — if 1 and 2 work but JDBC fails, it is the driver: add `trustServerCertificate=true;loginTimeout=90`.

Students do not need to memorise the code; they need the *method*: isolate the layers.

## Homework

1. Land `retail.customers` again but into a folder named with **yesterday's** date (`date.today() - timedelta(days=1)`). Confirm both folders exist.
2. Write down in one paragraph what "land raw first" protects you from.
3. Change `dbtable` to a query: `.option("query", "SELECT customer_id, state FROM retail.customers WHERE state = 'CA'")` and count rows.

## Common problems

* Wrong `databaseName` gives the same generic error as a wrong password — apply the three-step method.
* Forgetting `mode("overwrite")` → "path already exists" on the second run.
* Students write to `/Volumes/.../raw_data/` root — insist on the `<source>/load_date=` structure from day one; the whole platform depends on it.
