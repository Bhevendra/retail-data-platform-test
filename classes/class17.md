# Class 17 — Logging, exceptions and observability

## Objectives

* Replace `print` with the `logging` module and explain levels and structured (JSON) logs.
* Use `try / except / raise` deliberately: catch to add context, re-raise to fail the run.
* Write a context manager (`with track_entity(...)`) that records success/failure.
* Build `RunContext` and the `ops.pipeline_runs` table; adopt "process everything, report all failures at the end".

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | Why prints are not enough (find the failing entity in a 3,000-line job log) |
| 10–35 | Mini-lesson: logging; JSON formatter |
| 35–60 | Mini-lesson: exceptions (`try/except/finally/raise`, custom messages) |
| 60–80 | `RunContext` + `widget_context` |
| 80–95 | `track_entity` context manager + `ops.pipeline_runs`; "collect failures" loop |
| 95–100 | Homework |

## Mini-lesson: logging (25 min)

Ladder on one function:

```python
import logging
logger = logging.getLogger("common_utils")
logger.setLevel(logging.INFO)

def land(df, source_name):
    logger.info("landing %s", source_name)        # instead of print
    logger.warning("no rows for %s", source_name)
    logger.error("landing failed for %s", source_name)
```

Levels: DEBUG < INFO < WARNING < ERROR. A job runs at INFO; you turn on DEBUG only when
investigating. Then the JSON formatter from `common_utils/runtime.py`:

```python
from common_utils.runtime import get_logger, log
logger = get_logger()
log(logger, "source landed", source="s3_sales", rows=352, run_id="abc")
```

Output is one JSON object per line: machines (Databricks log search, a SIEM) can filter
`rows > 0 AND source = 's3_sales'`. Show the class the actual job output from the
project's `s2g` run — every line has `run_id`, `entity`, `layer`.

## Mini-lesson: exceptions (25 min)

Ladder with the raw path check from Class 6 homework:

```python
# v1: crash with Spark's message
df = spark.read.parquet(path)

# v2: catch and hide -> WRONG (silent failure)
try:
    df = spark.read.parquet(path)
except Exception:
    df = None

# v3: catch, add context, re-raise
try:
    df = spark.read.parquet(path)
except Exception as exc:
    raise FileNotFoundError(f"Raw landing path is missing: {path}. Run the ingestion task for '{source_name}' with run_date={run_date} before Bronze.") from exc
```

Rules on the board: never swallow an exception silently; catch only to add context or to
continue with *other* work; `finally` for cleanup (`client.close()` in `read_cosmos`);
raise `ValueError` for bad input/config, `FileNotFoundError` for missing data,
`RuntimeError` for "the run as a whole failed".

## `RunContext` (20 min)

Every notebook needs the same five values: environment, catalog, secret scope, run date,
run id. A frozen dataclass carries them:

```python
from common_utils.runtime import widget_context
ctx = widget_context(dbutils, task="ds2b", defaults={"environment": "dev", "catalog": "retaildataplatform",
                     "secret_scope": "retail-platform-dev", "run_date": date.today().isoformat()})
print(ctx.run_id, ctx.run_date_iso, ctx.as_dict())
```

Read `widget_context` together: creates widgets with defaults (interactive), reads them
(job parameters overwrite defaults), parses the date, tries to fetch the job run id.
`parse_run_date` accepts `""` and `{{job.start_time.iso_date}}` — Class 20 explains that
placeholder.

## `track_entity` and `ops.pipeline_runs` (15 min)

Mini-lesson: a context manager is code that runs *before and after* a block, even if the
block fails — `with open(...)` is one. Ours records a row per entity:

```python
from common_utils.observability import ensure_ops_schema, track_entity
ensure_ops_schema(spark, ctx.catalog, "ops")

failures = {}
for source in config.sources:
    try:
        with track_entity(spark, ctx, ctx.catalog, "ops", layer="bronze", entity=source.target_table) as run:
            result = load_source_to_bronze(spark, dbutils, ctx, config, source, volume_path)
            run.rows_read, run.rows_written = result.rows_read, result.rows_written
    except Exception as exc:
        failures[source.target_table] = f"{type(exc).__name__}: {exc}"

if failures:
    raise RuntimeError(f"DS2B finished with failures for {len(failures)} of {len(config.sources)} sources: {failures}")
```

Read `observability.py`: `@contextmanager`, `yield run`, the `except` that writes
FAILED then re-raises, the SUCCEEDED write after. Then the pattern above: one bad source
does not stop the others, but the run still fails with a full list. Query:

```sql
SELECT layer, entity, status, rows_read, rows_written, duration_seconds, error_message
FROM retaildataplatform.ops.pipeline_runs WHERE run_date = current_date() ORDER BY started_at;
```

## Homework

1. Convert `b2s` and `s2g` to `RunContext` + `track_entity` (note `s2g` stops at the first failure — explain why in a comment).
2. Make a source fail on purpose (wrong table name) and show the `ops.pipeline_runs` row and the final `RuntimeError`.
3. Write a one-paragraph incident note using only `ops.pipeline_runs` and the JSON logs.

## Common problems

* Logging twice — `get_logger` guards against adding two handlers; if students build their own, they will see duplicate lines.
* `except Exception:` without `as exc` loses the message — always bind it.
* Writing to `ops.pipeline_runs` inside `track_entity` when the ops schema does not exist — call `ensure_ops_schema` first (the notebooks do).
