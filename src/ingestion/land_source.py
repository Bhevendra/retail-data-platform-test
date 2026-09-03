# Databricks notebook source
# MAGIC %md
# MAGIC # Land source -> raw volume
# MAGIC One parameterised notebook for every source. `source_name` selects the entry in
# MAGIC `src/config/bronze.json`; the landing folder is `raw_data/<source>/load_date=<run_date>`
# MAGIC and is replaced on re-run (idempotent per date). No business logic lives here.

# COMMAND ----------

# MAGIC %pip install -q pymongo==4.10.1 boto3==1.35.98 databricks-sdk>=0.40.0

# COMMAND ----------

import sys
from datetime import date
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "retail_platform").is_dir():
        sys.path.insert(0, str(candidate))
        break

from retail_platform.config import load_bronze_config
from retail_platform.governance import bootstrap_namespace
from retail_platform.observability import ensure_ops_schema, track_entity
from retail_platform.runtime import get_logger, log, widget_context
from retail_platform.sources import extract_and_land

# COMMAND ----------

dbutils.widgets.text("config_path", "src/config/bronze.json")
dbutils.widgets.text("source_name", "sqlserver_customers")
ctx = widget_context(
    dbutils,
    task="land_source",
    defaults={"environment": "dev", "catalog": "retaildataplatform", "secret_scope": "retail-platform-dev", "run_date": date.today().isoformat()},
)
config = load_bronze_config(dbutils.widgets.get("config_path"))
source = config.source(dbutils.widgets.get("source_name"))
logger = get_logger()

# COMMAND ----------

bootstrap_namespace(spark, ctx.catalog, config.schema, config.raw_volume, comment="Bronze: immutable raw copies of source data with audit columns")
ensure_ops_schema(spark, ctx.catalog, config.platform.ops_schema)
volume_path = f"/Volumes/{ctx.catalog}/{config.schema}/{config.raw_volume}"

with track_entity(spark, ctx, ctx.catalog, config.platform.ops_schema, layer="raw", entity=source.name) as run:
    path, rows = extract_and_land(spark, dbutils, source, ctx.secret_scope, volume_path, ctx.run_date_iso)
    run.rows_read = rows
    run.rows_written = rows
    log(logger, "source landed", source=source.name, path=path, rows=rows, **ctx.as_dict())

dbutils.notebook.exit(path)
