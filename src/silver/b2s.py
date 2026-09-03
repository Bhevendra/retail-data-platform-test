# Databricks notebook source
# MAGIC %md
# MAGIC # B2S — Bronze -> Silver
# MAGIC Applies the configured transformations, quality rules and SCD type 1 / type 2
# MAGIC merge for each entity in `src/config/silver.json`. Only the Bronze rows of
# MAGIC `run_date` are processed, so the step is idempotent and cheap to re-run.

# COMMAND ----------

import sys
from datetime import date
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "retail_platform").is_dir():
        sys.path.insert(0, str(candidate))
        break

from retail_platform.config import load_silver_config
from retail_platform.governance import apply_grants, bootstrap_namespace
from retail_platform.observability import ensure_ops_schema, track_entity
from retail_platform.quality import ensure_results_table
from retail_platform.runtime import get_logger, log, widget_context
from retail_platform.silver import load_entity

# COMMAND ----------

dbutils.widgets.text("config_path", "src/config/silver.json")
dbutils.widgets.text("entities", "")  # optional comma-separated subset
dbutils.widgets.text("detect_deletes", "false")  # close SCD2 rows whose key vanished from a full extract
ctx = widget_context(
    dbutils,
    task="b2s",
    defaults={"environment": "dev", "catalog": "retaildataplatform", "secret_scope": "retail-platform-dev", "run_date": date.today().isoformat()},
)
config = load_silver_config(dbutils.widgets.get("config_path"))
selected = {s.strip() for s in dbutils.widgets.get("entities").split(",") if s.strip()}
entities = [e for e in config.entities if not selected or e.target_table in selected]
detect_deletes = dbutils.widgets.get("detect_deletes").lower() == "true"
logger = get_logger()

# COMMAND ----------

bootstrap_namespace(spark, ctx.catalog, config.silver_schema, comment="Silver: conformed, typed, de-duplicated entities with SCD history")
ensure_ops_schema(spark, ctx.catalog, config.platform.ops_schema)
ensure_results_table(spark, ctx.catalog, config.platform.ops_schema)

failures: dict[str, str] = {}
for entity in entities:
    try:
        with track_entity(spark, ctx, ctx.catalog, config.platform.ops_schema, layer="silver", entity=entity.target_table) as run:
            result = load_entity(spark, ctx, config, entity, detect_deletes=detect_deletes)
            run.rows_read, run.rows_written = result.rows_read, result.rows_written
    except Exception as exc:  # noqa: BLE001
        failures[entity.target_table] = f"{type(exc).__name__}: {exc}"

apply_grants(spark, ctx.catalog, config.silver_schema, config.platform.grants)
if failures:
    raise RuntimeError(f"B2S finished with failures for {len(failures)} of {len(entities)} entities: {failures}")
log(logger, "B2S completed", entities=[e.target_table for e in entities], **ctx.as_dict())
