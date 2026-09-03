# Databricks notebook source
# MAGIC %md
# MAGIC # Amazon S3 to Unity Catalog volume
# MAGIC Copies the original Parquet object into the volume with no format conversion.

# COMMAND ----------

import sys
from datetime import date
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "common_utils").is_dir():
        sys.path.insert(0, str(candidate))
        break

from common_utils.config import load_config
from common_utils.governance import bootstrap_namespace
from common_utils.sources import land_raw, read_source

dbutils.widgets.text("config_path", "src/bronze/config/bronze.json")
dbutils.widgets.text("catalog", "retaildataplatform")
dbutils.widgets.text("secret_scope", "retail-platform-dev")

config = load_config(dbutils.widgets.get("config_path"))
catalog = dbutils.widgets.get("catalog") or config["platform"]["catalog"]
platform = config["platform"]
source = next(item for item in config["sources"] if item["type"] == "s3")
bootstrap_namespace(spark, catalog, platform["schema"], platform["raw_volume"])
df = read_source(spark, dbutils, source, dbutils.widgets.get("secret_scope"))
path = land_raw(spark, dbutils, df, source, f"/Volumes/{catalog}/{platform['schema']}/{platform['raw_volume']}", date.today().isoformat())
print(f"S3 data landed to {path}")
