# Databricks notebook source
# MAGIC %md
# MAGIC # B2S — Bronze to Silver
# MAGIC Applies a per-entity SCD type 1 or type 2 merge selected in configuration.

# COMMAND ----------

import os
import sys
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "common_utils").is_dir():
        sys.path.insert(0, str(candidate))
        break
from common_utils.config import load_config
from common_utils.governance import bootstrap_namespace
from common_utils.scd import merge_type_1, merge_type_2, row_hash

dbutils.widgets.text("config_path", "src/silver/config/silver.json")
dbutils.widgets.text("catalog", "retaildataplatform")
config = load_config(dbutils.widgets.get("config_path"))
catalog = dbutils.widgets.get("catalog") or config["platform"]["catalog"]
platform = config["platform"]
bootstrap_namespace(spark, catalog, platform["silver_schema"])

for entity in config["entities"]:
    bronze = spark.table(f"{catalog}.{platform['bronze_schema']}.{entity['source_table']}")
    business_columns = [c for c in bronze.columns if c not in entity.get("exclude_from_hash", [])]
    prepared = row_hash(bronze, business_columns)
    target = f"{catalog}.{platform['silver_schema']}.{entity['target_table']}"
    if entity["scd_type"] == 1:
        merge_type_1(spark, prepared, target, entity["primary_keys"])
    elif entity["scd_type"] == 2:
        merge_type_2(spark, prepared, target, entity["primary_keys"])
    else:
        raise ValueError(f"Unsupported SCD type for {entity['target_table']}: {entity['scd_type']}")

print("B2S completed successfully.")
