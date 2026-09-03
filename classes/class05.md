# Class 5 — Secrets, functions and the first config.json

The most important class of Phase 1: the three hard-coded scripts become one
parameterised, config-driven notebook — the exact `sum2no` ladder applied to real code.

## Objectives

* Store credentials in a Databricks secret scope and read them with `dbutils.secrets.get`.
* Turn each ingestion script into a function with parameters.
* Describe sources in `config.json` and select one by name.
* Build the first version of `land_source` (one notebook, one `source_name` widget).

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: three notebooks, same shape, three passwords in plain text |
| 10–30 | Mini-lesson + hands-on: secret scopes |
| 30–55 | Ladder step 2–3: scripts → functions with prints |
| 55–80 | Mini-lesson: JSON config; ladder step 4 |
| 80–95 | Assemble `land_source` v1 with a `source_name` widget |
| 95–100 | Homework |

## Mini-lesson: secret scopes (20 min)

Rule: **code is shared, secrets are not**. A secret scope is a locked drawer the
notebook can open but cannot print.

Set up once with the Databricks CLI (instructor shares screen; students repeat):

```bash
databricks secrets create-scope retail-platform-dev
databricks secrets put-secret retail-platform-dev sqlserver-username
databricks secrets put-secret retail-platform-dev sqlserver-password
databricks secrets put-secret retail-platform-dev aws-access-key-id
databricks secrets put-secret retail-platform-dev aws-secret-access-key
databricks secrets put-secret retail-platform-dev cosmos-connection-string
```

In the notebook:

```python
user = dbutils.secrets.get(scope="retail-platform-dev", key="sqlserver-username")
print(user)          # prints [REDACTED]
print(len(user))     # the value is really there
```

Replace every hard-coded credential in the three notebooks. Explain the naming
convention (`<system>-<what>`) — it becomes a documented contract in Class 25.

## Ladder step 2 and 3: functions (25 min)

Take the SQL Server script from Class 2 and do the transformation live, exactly like `sum2no`:

```python
def read_sqlserver(host, database, table, username, password):
    print("connecting to:", host, "database:", database, "table:", table)
    url = f"jdbc:sqlserver://{host}:1433;databaseName={database};encrypt=true;trustServerCertificate=true;loginTimeout=90"
    df = (spark.read.format("jdbc").option("url", url).option("dbtable", table)
          .option("user", username).option("password", password)
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())
    print("rows read:", df.count())
    return df

def land_csv(df, source_name, run_date):
    target = f"/Volumes/retaildataplatform/bronze/raw_data/{source_name}/load_date={run_date}"
    print("landing to:", target)
    df.write.mode("overwrite").option("header", "true").option("quoteAll", "true").option("escape", '"').csv(target)
    return target

df = read_sqlserver("rivadata.database.windows.net", "batch2", "retail.customers",
                    dbutils.secrets.get("retail-platform-dev", "sqlserver-username"),
                    dbutils.secrets.get("retail-platform-dev", "sqlserver-password"))
path = land_csv(df, "sqlserver_customers", "2026-09-03")
```

Two things to emphasise: `return` (the function hands the DataFrame back instead of
printing it) and that `land_csv` knows nothing about SQL Server — it will work for any
DataFrame. Students then write `read_cosmos(...)` and `land_json(...)` themselves (15 min,
with the Class 4 code in front of them).

## Mini-lesson: configuration files (25 min)

Ask: what changes between the three notebooks? Only *values*: host, table, secret key
names, format. What stays? The *steps*. Values belong in a file, steps in code.

Create `src/config/bronze.json` (first version, small):

```json
{
  "platform": {"catalog": "retaildataplatform", "schema": "bronze", "raw_volume": "raw_data"},
  "sources": [
    {"name": "sqlserver_customers", "type": "sqlserver", "host": "rivadata.database.windows.net",
     "database": "batch2", "table": "retail.customers", "format": "csv",
     "secret_keys": {"username": "sqlserver-username", "password": "sqlserver-password"}},
    {"name": "s3_sales", "type": "s3", "path": "s3://bucket-rivadata/retail/sales.parquet",
     "region": "eu-north-1", "format": "parquet",
     "secret_keys": {"access_key_id": "aws-access-key-id", "secret_access_key": "aws-secret-access-key"}},
    {"name": "cosmos_sales_orders", "type": "cosmos_mongodb", "database": "retail",
     "collection": "sales_orders", "format": "json",
     "secret_keys": {"connection_string": "cosmos-connection-string"}}
  ]
}
```

Read it:

```python
import json
with open("src/config/bronze.json") as handle:
    config = json.load(handle)

print(type(config), config.keys())
for source in config["sources"]:
    print(source["name"], "->", source["type"])
```

JSON → dictionary → lists of dictionaries: they already know all three. Then the
selection pattern they will use for the rest of the course:

```python
source_name = "sqlserver_customers"
source = next(s for s in config["sources"] if s["name"] == source_name)
print(source)
```

Explain `next(... for ... if ...)` as "the first item that matches".

## Assemble `land_source` v1 (15 min)

```python
dbutils.widgets.text("source_name", "sqlserver_customers")
dbutils.widgets.text("run_date", date.today().isoformat())
source_name = dbutils.widgets.get("source_name")
run_date = dbutils.widgets.get("run_date")
scope = "retail-platform-dev"
source = next(s for s in config["sources"] if s["name"] == source_name)

if source["type"] == "sqlserver":
    df = read_sqlserver(source["host"], source["database"], source["table"],
                        dbutils.secrets.get(scope, source["secret_keys"]["username"]),
                        dbutils.secrets.get(scope, source["secret_keys"]["password"]))
    land_csv(df, source["name"], run_date)
elif source["type"] == "cosmos_mongodb":
    df = read_cosmos(source["database"], source["collection"], dbutils.secrets.get(scope, source["secret_keys"]["connection_string"]))
    land_json(df, source["name"], run_date)
elif source["type"] == "s3":
    land_s3_object(source["path"], source["region"], dbutils.secrets.get(scope, source["secret_keys"]["access_key_id"]),
                   dbutils.secrets.get(scope, source["secret_keys"]["secret_access_key"]), source["name"], run_date)
else:
    print("unknown source type:", source["type"])
```

Run it three times with the widget set to each source. **One notebook, three sources.**
Compare with the reference `src/ingestion/land_source.ipynb`: the shape is the same;
the differences (logging, `RunContext`, `track_entity`, typed config) are Classes 16–17.

## Homework

1. Add a fourth (fake) entry to `bronze.json` with a type `"ftp"` and run the notebook — see the `else` branch fire. Then make the `else` `raise ValueError("Unsupported source type: " + source["type"])` and see how a raised error looks different from a print.
2. Write `land_parquet(df, source_name, run_date)`.
3. Delete all hard-coded credentials from every notebook you have; search the folder for the word "password".

## Common problems

* `dbutils.secrets.get` with a wrong key name → `Secret does not exist`; check `databricks secrets list-secrets retail-platform-dev`.
* `open("src/config/bronze.json")` fails when the notebook's working directory is not the repo root — Class 16 fixes this properly; for now run notebooks from the repo folder.
* Students put the `next(...)` inside the `if` chain — keep selection (find the source) separate from action (read/land).
