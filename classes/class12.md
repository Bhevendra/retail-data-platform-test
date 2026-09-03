# Class 12 — Assembling the Silver notebook

## Objectives

* Combine `apply_transformations`, `row_hash`, `deduplicate` and the two merges into one `prepare` + `load_entity` flow.
* Drive it from `silver.json` for all three entities with a `for` loop.
* Process only the Bronze rows of the current `run_date`.
* Attach lineage columns and understand which columns are excluded from the hash.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: the pieces we have |
| 10–35 | `prepare(df, entity, run_date, run_id)` |
| 35–55 | `load_entity` with SCD type chosen from config |
| 55–75 | `b2s` v1 loop; run for all entities; re-run |
| 75–90 | Read the reference `common_utils/silver.py` together |
| 90–95 | Homework |

## `prepare` (25 min)

```python
AUDIT = ["_load_date", "_ingested_at", "_run_id", "_source_system", "_source_file", "last_update_ts", "file_path"]
SCD2 = ["effective_from", "effective_to", "is_current"]

def business_columns(df, entity):
    excluded = set(entity.get("exclude_from_hash", [])) | set(AUDIT) | set(SCD2) | {"_row_hash", "_silver_updated_at"}
    return [c for c in df.columns if c not in excluded]

def prepare(df, entity, run_date, run_id):
    shaped = apply_transformations(df, entity.get("transformations", {}))
    missing = [k for k in entity["primary_keys"] if k not in shaped.columns]
    if missing:
        raise ValueError(f"{entity['target_table']}: primary key columns missing after transformations: {missing}")
    hashed = row_hash(shaped, business_columns(shaped, entity))
    deduped = deduplicate(hashed, entity["primary_keys"], entity.get("order_by", "_ingested_at"))
    keep = [c for c in deduped.columns if c not in AUDIT or c in ("_run_id", "_load_date")]
    return (deduped.select(*keep)
            .withColumn("_run_id", F.lit(run_id))
            .withColumn("_load_date", F.lit(run_date).cast("date"))
            .withColumn("_silver_updated_at", F.current_timestamp()))
```

Discuss the `raise ValueError(...)` — the first deliberate exception in the course: a
config mistake (key column renamed away) should stop the entity with a message naming
the problem, not produce a table with a NULL key. Sets (`set(...) | set(...)`) are new:
"a list without duplicates that is fast to check membership".

## `load_entity` (20 min)

```python
def load_entity(entity, run_date, run_id):
    bronze = spark.table(f"retaildataplatform.bronze.{entity['source_table']}")
    batch = bronze.filter(F.col("_load_date") == F.lit(run_date).cast("date"))
    prepared = prepare(batch, entity, run_date, run_id)
    rows = prepared.count()
    if rows == 0:
        print("no rows for", run_date, "- skipping", entity["target_table"])
        return 0
    target = f"`retaildataplatform`.`silver`.`{entity['target_table']}`"
    if entity["scd_type"] == 1:
        merge_type_1(prepared, target, entity["primary_keys"])
    elif entity["scd_type"] == 2:
        merge_type_2(prepared, target, entity["primary_keys"])
    else:
        raise ValueError(f"Unsupported SCD type: {entity['scd_type']}")
    return rows
```

Why filter on `_load_date`? Bronze holds every day; Silver processes *one* day per run,
so a backfill of 1 September does not re-merge today's data. Idempotency per date again.

Remind the class: **no `.cache()`** — on Serverless it raises `PERSIST TABLE is not
supported`; that was a real failure in the project.

## `b2s` v1 (20 min)

```python
import json, uuid
from datetime import date
dbutils.widgets.text("run_date", date.today().isoformat())
run_date = dbutils.widgets.get("run_date"); run_id = str(uuid.uuid4())
with open("src/config/silver.json") as h:
    config = json.load(h)
spark.sql("CREATE SCHEMA IF NOT EXISTS retaildataplatform.silver")

for entity in config["entities"]:
    print("=== entity:", entity["target_table"], "scd", entity["scd_type"])
    rows = load_entity(entity, run_date, run_id)
    print("rows processed:", rows)
```

Checkpoints: `silver.customers` 28,670 current rows; `silver.sales_orders` 4,000; `silver.sales` 352. Run again — `DESCRIBE HISTORY` shows merges with zero changes.

Compare with `src/silver/b2s.ipynb`: same loop; extras are the `entities` widget for
subset runs, `detect_deletes` widget, quality rules, governance, `track_entity`, and the
"collect failures, raise at the end" pattern.

## Reading the reference (15 min)

Open `common_utils/silver.py` and read `prepare` and `load_entity` side by side with the
class version. Differences students should be able to name: typed `entity.transformations`
instead of a dict (Class 16), `tolerant_cast` (Class 9), quality evaluation (Class 18),
`govern_table` (Class 19), `SilverLoadResult` dataclass (Class 16).

## Homework

1. Add `"exclude_from_hash": ["units_purchased"]` to `customers` in `silver.json`, reload, and explain why a change in `units_purchased` no longer creates a new version. Then remove it again.
2. Run `b2s` for a date with no Bronze rows and confirm the skip message.
3. Write a SQL query that shows, per Silver table, the number of current vs closed rows.

## Common problems

* A Silver table created before a config change has old column names → drop the Silver table and rebuild (allowed in dev; Class 25 discusses migrations).
* `order_by` column not present after transformations (e.g. renamed) → `deduplicate` silently falls back to `dropDuplicates`; check the config names.
* Students forget `CREATE SCHEMA ... silver` — the reference `bootstrap_namespace` handles it (Class 19).
