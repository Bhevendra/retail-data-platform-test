# Databricks notebook source
# MAGIC %md
# MAGIC # S2G — Silver to Gold
# MAGIC Creates governed BI/AI serving views from version-controlled SQL definitions.

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

dbutils.widgets.text("config_path", "src/gold/config/gold.json")
dbutils.widgets.text("catalog", "retaildataplatform")
config = load_config(dbutils.widgets.get("config_path"))
catalog = dbutils.widgets.get("catalog") or config["platform"]["catalog"]
platform = config["platform"]
bootstrap_namespace(spark, catalog, platform["gold_schema"])

for product in config["products"]:
    if product["type"] != "view":
        raise ValueError(f"Unsupported gold product type: {product['type']}")
    sql = product["sql"].replace("${catalog}", catalog)
    spark.sql(f"CREATE OR REPLACE VIEW `{catalog}`.`{platform['gold_schema']}`.`{product['name']}` AS {sql}")
    for key, value in platform.get("tags", {}).items():
        spark.sql(f"ALTER VIEW `{catalog}`.`{platform['gold_schema']}`.`{product['name']}` SET TAGS ('{key}' = '{value}')")

print("S2G completed successfully.")
