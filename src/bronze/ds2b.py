# Databricks notebook source
# MAGIC %md
# MAGIC # DS2B — raw volume -> Bronze
# MAGIC Reads the files landed for `run_date`, adds audit columns, evaluates data-quality
# MAGIC rules (fail / quarantine / warn per source) and writes idempotently per load date.
# MAGIC Every source is processed even if one fails; the notebook raises at the end so the
# MAGIC job shows exactly which entities need attention in `ops.pipeline_runs`.

# COMMAND ----------

import sys
from datetime import date
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "retail_platform").is_dir():
        sys.path.insert(0, str(candidate))
        break

from retail_platform.bronze import load_source_to_bronze
from retail_platform.config import load_bronze_config
from retail_platform.governance import apply_grants, bootstrap_namespace
from retail_platform.observability import ensure_ops_schema, track_entity
from retail_platform.quality import ensure_results_table
from retail_platform.runtime import get_logger, log, widget_context

# COMMAND ----------

dbutils.widgets.text("config_path", "src/config/bronze.json")
dbutils.widgets.text("sources", "")  # optional comma-separated subset for targeted re-runs
ctx = widget_context(
    dbutils,
    task="ds2b",
    defaults={"environment": "dev", "catalog": "retaildataplatform", "secret_scope": "retail-platform-dev", "run_date": date.today().isoformat()},
)
config = load_bronze_config(dbutils.widgets.get("config_path"))
selected = {s.strip() for s in dbutils.widgets.get("sources").split(",") if s.strip()}
sources = [s for s in config.sources if not selected or s.name in selected]
logger = get_logger()

# COMMAND ----------

bootstrap_namespace(spark, ctx.catalog, config.schema, config.raw_volume, comment="Bronze: immutable raw copies of source data with audit columns")
ensure_ops_schema(spark, ctx.catalog, config.platform.ops_schema)
ensure_results_table(spark, ctx.catalog, config.platform.ops_schema)
volume_path = f"/Volumes/{ctx.catalog}/{config.schema}/{config.raw_volume}"

failures: dict[str, str] = {}
for source in sources:
    try:
        with track_entity(spark, ctx, ctx.catalog, config.platform.ops_schema, layer="bronze", entity=source.target_table) as run:
            result = load_source_to_bronze(spark, dbutils, ctx, config, source, volume_path)
            run.rows_read, run.rows_written = result.rows_read, result.rows_written
            log(logger, "bronze loaded", entity=result.entity, rows_read=result.rows_read, rows_written=result.rows_written, rows_quarantined=result.rows_quarantined)
    except Exception as exc:  # noqa: BLE001 - keep going, report all failures at the end
        failures[source.target_table] = f"{type(exc).__name__}: {exc}"

apply_grants(spark, ctx.catalog, config.schema, config.platform.grants)
if failures:
    raise RuntimeError(f"DS2B finished with failures for {len(failures)} of {len(sources)} sources: {failures}")
log(logger, "DS2B completed", sources=[s.name for s in sources], **ctx.as_dict())
