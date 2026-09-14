# Class 3 — From six copied notebooks to one function library

> The pivot class. Nothing new gets ingested today: the same six notebooks from Classes 1
> and 2 turn into a small library plus two thin notebooks driven by config. Every step
> removes a pain the students felt last week, in the order they felt it.

## Objectives

By the end of this class every student can:

* replace `print` with the `logging` module and say what each of the four levels is for;
* create a Python **module** and **package**, import it into a notebook, and explain
  `sys.path`;
* write a function with parameters and default values, and move working notebook code
  into it without changing behaviour;
* build the whole Bronze layer from **one** notebook driven by a dictionary;
* move that dictionary into a JSON config file and explain what that buys.

## The ladder we climb today

```
v1  prints, everything hard-coded, 6 notebooks      (where Class 2 ended)
v2  logging instead of prints, still 6 notebooks
v3  logging lives in common_utils/logger.py         → 1 change, 6 notebooks fixed
v4  ingestion code lives in common_utils/ingestors.py
v5  bronze_ingestor() in common_utils → 1 Bronze notebook + a dictionary
v6  the dictionary moves out into JSON config files
```

Write that on the board at minute 0 and tick each line as you finish it. The students
should feel the notebooks getting shorter while the *system* gets bigger.

## Time plan (120 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: "1 change = 3 edits". Today's ladder on the board |
| 10–20 | Mini-lesson: logging vs print |
| 20–35 | v2: logging in the ingestion and Bronze notebooks (hard-coded) |
| 35–45 | Mini-lesson: modules, packages, imports, `sys.path` |
| 45–60 | v3: `common_utils/logger.py`, imported everywhere |
| 60–80 | v4: ingestion functions in `common_utils/ingestors.py` |
| 80–100 | v5: `bronze_ingestor()` and one Bronze notebook with a dictionary |
| 100–115 | v6: the dictionary becomes `*.json` config files |
| 115–120 | Recap, homework |

**Natural break:** if the group is slow, stop after v4 and start next class at v5. Do not
stop in the middle of v5 — a half-converted Bronze notebook is worse than either end.

---

## 1. Mini-lesson: logging vs print (10 min)

Ask first: *"Your Bronze job failed last night at 03:12. What do the prints tell you?"*
Then list what `print` cannot do:

| | `print` | `logging` |
| --- | --- | --- |
| Timestamp | no | yes |
| Severity | no | INFO / WARNING / ERROR / DEBUG |
| Turn the noise down | delete the lines | change one level |
| Says which notebook wrote it | no | the logger name does |
| Full error with traceback | no | `logger.exception()` |

The four levels, in one sentence each:

* **DEBUG** — detail you want only while hunting a bug ("here is the path I built").
* **INFO** — the story of a normal run ("read 28,813 rows", "wrote table X").
* **WARNING** — odd but survivable ("source returned 0 rows").
* **ERROR** — it failed ("could not connect").

Rule of thumb they should write down: *if it prints on every successful run and you would
be annoyed to lose it, it is INFO; if it means someone should look, it is WARNING or
ERROR.*

## 2. v2 — logging, still inside each notebook (15 min)

Put this at the top of `ds2b_s3` and walk through it line by line:

```python
import logging

logger = logging.getLogger("ds2b_s3")
logger.setLevel(logging.INFO)

if not logger.handlers:                         # notebooks re-run cells; without this
    handler = logging.StreamHandler()           # you get the same line 2, 3, 4 times
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))
    logger.addHandler(handler)
```

Three ideas, no more:

* **the logger** is the thing you talk to, and it has a **name** (use the notebook's name,
  so the log line says where it came from);
* **the handler** decides where the line goes (screen, file, a log service);
* **the formatter** decides what the line looks like.

The `if not logger.handlers` guard is worth a full minute: run the cell twice without it
and every message appears twice. This is the single most common logging bug in notebooks.

Now replace the prints:

```python
raw_path = "/Volumes/retaildataplatform/bronze/raw_data/s3_sales/load_date=2026-09-14/"
logger.info("reading raw data from %s", raw_path)

df = spark.read.format("parquet").load(raw_path)
logger.info("read %s rows", df.count())

df = (df.withColumn("last_update_ts", F.current_timestamp())
        .withColumn("file_path", F.col("_metadata.file_path")))
logger.info("added audit columns")

target_table = "retaildataplatform.bronze.s3_sales"
df.write.format("delta").mode("overwrite").saveAsTable(target_table)
logger.info("wrote table %s", target_table)
```

Show `logger.info("read %s rows", count)` versus `logger.info(f"read {count} rows")` and
explain the `%s` style is the convention (the string is only built if the level is
actually on). Either works; be consistent.

Then make the point that sells the rest of the class: **that eleven-line setup block now
has to be pasted into six notebooks.** Ask what happens when they want the log lines to
include the run date. Six edits.

Checkpoint: everyone has timestamped, levelled output in `ds2b_s3`.

## 3. Mini-lesson: modules, packages, imports, `sys.path` (10 min)

* A **module** is a `.py` file. `logger.py` is a module.
* A **package** is a folder of modules with an `__init__.py` file in it. `common_utils/`
  is a package.
* `import` runs that file once and gives you the names defined in it.
* Python finds files to import by looking through a list of folders: `sys.path`. If your
  folder is not in that list, `import` fails with `ModuleNotFoundError` — which is not a
  mysterious error, it is Python saying "I looked in these places and it was not there".

Show it live:

```python
import sys
for p in sys.path:
    print(p)
```

Create the structure in the workspace (Workspace → Create → Folder / File):

```
bootcamp/
├── common_utils/
│   ├── __init__.py          (empty file — it makes the folder a package)
│   └── logger.py
├── s3-ingestion
├── cosmosdb-ingestion
├── sql-server-ingestion
└── ds2b_s3 ...
```

## 4. v3 — logging moves into `common_utils` (15 min)

`common_utils/logger.py`:

```python
"""Logging for every notebook in the project. Import it, do not copy it."""

import logging

FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def get_logger(name, level=logging.INFO):
    """Return a logger that prints one clean line per message.

    name  : where the message came from, e.g. "ds2b_s3"
    level : the lowest level you want to see (default INFO)
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(FORMAT))
        logger.addHandler(handler)

    return logger
```

The eleven lines from every notebook now live in exactly one file. In the notebook:

```python
import sys
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:      # walk up until we find the package
    if (candidate / "common_utils").is_dir():
        sys.path.insert(0, str(candidate))
        break

from common_utils.logger import get_logger

logger = get_logger("ds2b_s3")
logger.info("hello from the library")
```

Explain the loop in plain words: *"start where this notebook is, look for a folder called
`common_utils`, and if it is not here look one level up, and keep going."* Written this
way it keeps working when the notebook moves — which it will, in the class where we lay
out the real repository.

> **The gotcha that will eat ten minutes if you do not warn them:** edit `logger.py`, rerun
> the notebook cell, and nothing changes. Python imported the old version and kept it.
> Fix: run `dbutils.library.restartPython()` in a cell (or detach/reattach) after editing a
> module. Tell them now, before it happens.

Exercise (5 min): every student converts one more notebook to `get_logger`. Then change
`FORMAT` in `logger.py` — one edit — and watch every notebook's output change. Cross **"1
change = 3 edits"** off the board.

## 5. v4 — ingestion functions (20 min)

Now the same treatment for the reading code. `common_utils/ingestors.py`:

```python
"""Generic readers. Nothing in here knows anything about retail, customers or orders."""

from bson import json_util
from pymongo import MongoClient


def read_jdbc(spark, url, dbtable, user, password,
              driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"):
    """Read a table (or a query) from a relational database over JDBC."""
    return (spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", dbtable)
            .option("user", user)
            .option("password", password)
            .option("driver", driver)
            .load())


def read_mongo_as_json(spark, connection_string, database, collection, column="json_data"):
    """Read every document from a collection as one JSON string per row."""
    client = MongoClient(connection_string)
    documents = list(client[database][collection].find({}))
    rows = [(json_util.dumps(document),) for document in documents]
    return spark.createDataFrame(rows, [column])


def s3a_path(access_key, secret_key, bucket, key):
    """Build the s3a:// URL Spark uses to read from S3."""
    return f"s3a://{access_key}:{secret_key}@{bucket}/{key}"


def write_raw(df, target_path, file_format, mode="overwrite", options=None):
    """Land a DataFrame in the raw volume, in whatever format the source gave us."""
    writer = df.write.mode(mode).format(file_format)
    for key, value in (options or {}).items():
        writer = writer.option(key, value)
    writer.save(target_path)
    return target_path
```

Teach three Python ideas as they appear, not before:

1. **Default parameter values** — `mode="overwrite"`, `driver=...`. The caller can ignore
   them; when they need to change one, they pass it.
2. **`options=None` then `options or {}`** — why not `options={}` in the signature? Because
   a mutable default is shared between calls. Say it once, show the rule: *never put a
   list or dict as a default value.*
3. **The list comprehension** `[(json_util.dumps(d),) for d in documents]` — write the
   `for` loop from Class 1 next to it on the board and show they are the same thing.

The SQL Server ingestion notebook becomes:

```python
from common_utils.ingestors import read_jdbc, write_raw
from common_utils.logger import get_logger
from datetime import date

logger = get_logger("sql-server-ingestion")

url = "jdbc:sqlserver://<server>.database.windows.net:1433;databaseName=<db>;encrypt=true;trustServerCertificate=true;loginTimeout=90"
run_date = date.today().isoformat()

logger.info("reading retail.customers from SQL Server")
df = read_jdbc(spark, url, "retail.customers", "<username>", "<password>")
logger.info("read %s rows", df.count())

target_path = f"/Volumes/retaildataplatform/bronze/raw_data/sqlserver_customers/load_date={run_date}"
write_raw(df, target_path, "csv", options={"header": "true", "quoteAll": "true", "escape": '"'})
logger.info("landed at %s", target_path)
```

Twelve lines instead of forty, and every line is about *this source* — which is the test
for whether a function was worth writing.

**The rule for `common_utils`, and repeat it all course:** nothing in there may mention
retail, customers, orders or this project. `read_jdbc` would work for a hospital or a
bank. Project-specific values stay in the notebook. *Generic code in the library, business
decisions in the layer.*

## 6. v5 — `bronze_ingestor()` and one Bronze notebook (20 min)

Look at the three `ds2b` notebooks side by side and circle what differs: the raw path, the
format, the read options, the table name. Everything else is identical — so everything
else becomes a function.

`common_utils/bronze.py`:

```python
"""Raw files → Bronze Delta table. Identical for every source, so it lives here once."""

import pyspark.sql.functions as F

LAST_UPDATE_TS = "last_update_ts"
FILE_PATH = "file_path"


def read_raw(spark, raw_path, file_format, options=None):
    """Read landed files of any format from the raw volume."""
    reader = spark.read.format(file_format)
    for key, value in (options or {}).items():
        reader = reader.option(key, value)
    return reader.load(raw_path)


def bronze_ingestor(df, target_table, mode="overwrite"):
    """Add the audit columns and write the DataFrame as a Bronze Delta table.

    df           : the DataFrame read from the raw volume
    target_table : catalog.schema.table
    mode         : "overwrite" (replace) or "append" (add)

    Returns the number of rows written.
    """
    df = (df.withColumn(LAST_UPDATE_TS, F.current_timestamp())
            .withColumn(FILE_PATH, F.col("_metadata.file_path")))

    df.write.format("delta").mode(mode).saveAsTable(target_table)
    return df.count()
```

Then **one** notebook replaces all three. The dictionary at the top is the only part that
changes from source to source:

```python
from common_utils.bronze import bronze_ingestor, read_raw
from common_utils.logger import get_logger

logger = get_logger("raw_to_bronze")

run_date = "2026-09-14"
catalog = "retaildataplatform"

SOURCES = {
    "s3_sales": {
        "raw_folder": "s3_sales",
        "file_format": "parquet",
        "read_options": {},
        "target_table": f"{catalog}.bronze.s3_sales",
        "mode": "overwrite",
    },
    "cosmosdb_sales_orders": {
        "raw_folder": "cosmosdb_sales_orders",
        "file_format": "json",
        "read_options": {},
        "target_table": f"{catalog}.bronze.cosmosdb_sales_orders",
        "mode": "overwrite",
    },
    "sqlserver_customers": {
        "raw_folder": "sqlserver_customers",
        "file_format": "csv",
        "read_options": {"header": "true", "inferSchema": "true"},
        "target_table": f"{catalog}.bronze.sqlserver_customers",
        "mode": "overwrite",
    },
}

for source_name, config in SOURCES.items():
    raw_path = f"/Volumes/{catalog}/bronze/raw_data/{config['raw_folder']}/load_date={run_date}"
    logger.info("[%s] reading %s", source_name, raw_path)

    df = read_raw(spark, raw_path, config["file_format"], config["read_options"])
    rows = bronze_ingestor(df, config["target_table"], config["mode"])

    logger.info("[%s] wrote %s rows to %s", source_name, rows, config["target_table"])

logger.info("bronze finished for %s sources", len(SOURCES))
```

Teach the dictionary properly, because everything from here on is built on it:

* a dictionary of dictionaries: the outer key is the source name, the inner dictionary is
  that source's settings;
* `SOURCES.items()` gives `(key, value)` pairs — that is how the loop gets both;
* `config["file_format"]` is a lookup, and a typo there gives `KeyError: 'file_fomat'`,
  which is Python telling you exactly what is wrong.

Ask the killer question: **"A fourth source arrives on Monday. What do you do?"** Add four
lines to the dictionary. No new notebook, no new code. That is the payoff of the whole
class.

Checkpoint: drop the three Bronze tables, run the single notebook, and watch all three
rebuild with one log line each.

### Stretch (if the room is fast): one failure should not kill the rest

```python
failures = {}

for source_name, config in SOURCES.items():
    try:
        ...
    except Exception as error:
        logger.exception("[%s] failed", source_name)
        failures[source_name] = str(error)

if failures:
    raise RuntimeError(f"bronze failed for {len(failures)} of {len(SOURCES)} sources: {failures}")
```

Two ideas: `logger.exception` prints the full traceback, and collecting failures means one
broken source does not hide the other two. The job still fails at the end — loudly, once,
with the whole picture.

## 7. v6 — the dictionary becomes config files (15 min)

Now take the last project-specific thing out of the code.

```
bootcamp/
├── common_utils/
│   ├── __init__.py
│   ├── logger.py
│   ├── ingestors.py
│   └── bronze.py
├── config/
│   ├── s3_sales.json
│   ├── cosmosdb_sales_orders.json
│   └── sqlserver_customers.json
└── notebooks...
```

`config/sqlserver_customers.json`:

```json
{
  "source_name": "sqlserver_customers",
  "description": "CRM customer master from Azure SQL Server",
  "source_type": "jdbc",
  "connection": {
    "url": "jdbc:sqlserver://<server>.database.windows.net:1433;databaseName=<db>;encrypt=true;trustServerCertificate=true;loginTimeout=90",
    "dbtable": "retail.customers"
  },
  "raw_folder": "sqlserver_customers",
  "landing_format": "csv",
  "landing_options": { "header": "true", "quoteAll": "true", "escape": "\"" },
  "read_options": { "header": "true", "inferSchema": "true" },
  "target_table": "retaildataplatform.bronze.sqlserver_customers",
  "mode": "overwrite"
}
```

Reading it is three lines:

```python
import json
from pathlib import Path

config = json.loads(Path("config/sqlserver_customers.json").read_text())
print(config["target_table"])
```

`json.loads` turns JSON text into exactly the Python dictionary they just wrote by hand —
show them `type(config)` and let that land. **JSON is a dictionary that lives in a file.**

The Bronze notebook now loads the whole folder:

```python
CONFIG_FOLDER = Path("config")

configs = [json.loads(p.read_text()) for p in sorted(CONFIG_FOLDER.glob("*.json"))]
logger.info("found %s source configs", len(configs))

for config in configs:
    raw_path = f"/Volumes/{catalog}/bronze/raw_data/{config['raw_folder']}/load_date={run_date}"
    df = read_raw(spark, raw_path, config["file_format"], config.get("read_options"))
    rows = bronze_ingestor(df, config["target_table"], config.get("mode", "overwrite"))
    logger.info("[%s] wrote %s rows", config["source_name"], rows)
```

`config.get("mode", "overwrite")` versus `config["mode"]` is worth thirty seconds: `.get`
returns a default instead of raising when the key is missing, so optional settings stay
optional.

### Why bother? (the part they must be able to argue in an interview)

* **A new source is a new file, not a code change.** Adding a source cannot break the
  three that already work, because you did not touch the code that runs them.
* **Reviewable by people who do not read Python.** A data owner can check a JSON file.
* **Same code, different environments.** Dev and prod differ by which config is passed in,
  not by an edited notebook.
* **The config is documentation.** `sqlserver_customers.json` says exactly what that feed
  is, in one screen.
* **Git diffs mean something.** "changed `mode` to append" is a reviewable sentence; a
  diff inside a 200-line notebook is not.

And the limits, so they do not over-apply it — this comes back later in the course:

* **Secrets never go in config.** A config holds the *name* of a secret; the value comes
  from a secret scope (next class).
* **Config is for things that repeat.** Three sources with the same shape: config. Three
  entities each needing different cleaning: that is code, and pretending it is config
  produces SQL buried in JSON that nobody can read.

## 8. Recap (5 min)

Count the lines with them: Class 2 ended with six notebooks of roughly forty lines each
(~240 lines, three of them near-duplicates). They now have four small library files, two
notebooks and three config files — and adding a fourth source touches **one new JSON file
and nothing else.**

Read the ladder off the board one more time; tell them every future feature (secrets,
idempotent loads, quality rules, governance) enters the project the same way: hard-coded
first, then a function, then the library, then config.

## Homework

1. Add `logger.debug("raw path is %s", raw_path)` to the Bronze notebook. Run it — nothing
   appears. Now get it to appear without editing the notebook. (Hint: `get_logger` takes a
   `level`.) Write down what you changed and why DEBUG is off by default.
2. Convert the two remaining ingestion notebooks (Cosmos, S3) to use `get_logger` and the
   functions in `ingestors.py`. Nothing may be pasted; if you want the same code twice,
   that is a function.
3. Add a fourth "source" to the Bronze run: re-land `retail.customers` as
   `sqlserver_customers_backup`, write its config file, and prove the Bronze notebook
   picks it up **without editing a single line of the notebook.**
4. Break it on purpose: put `"file_format": "csv"` in the Cosmos config and run it. Read
   the error out loud, write down what it told you, then fix it.
5. Write in your own words, five bullets maximum: what `common_utils` is allowed to
   contain and what it is not. Why does `bronze_ingestor` belong there but
   `"retaildataplatform.bronze.s3_sales"` does not?
6. **Stretch** — implement the try/except version of the Bronze loop from §6 and prove it:
   point one config at a folder that does not exist and confirm the other three still load
   and the notebook still fails at the end.

## Common problems

* `ModuleNotFoundError: No module named 'common_utils'` — the bootstrap loop did not find
  the folder. Print `Path.cwd()` and `sys.path` and look together; this is the best
  possible moment to teach that error rather than the worst.
* Edited a module, nothing changed → `dbutils.library.restartPython()`.
* Every log line appears twice or three times → the handler was added again; the
  `if not logger.handlers` guard is missing, or the logger was created twice under
  different names.
* `KeyError: 'read_options'` after moving to JSON — the key exists in two configs and not
  the third. Either add it everywhere or use `.get()`. Good moment to mention that a later
  class writes a *test* that checks every config has the same keys.
* Invalid JSON (trailing comma, single quotes, an unescaped `"` in the `escape` option).
  Show the error message, then show the editor highlighting the line.
* Someone puts the password into the JSON. Stop the class and make the point: config goes
  into git, and git is forever. Secret names only — the fix is next class.
