# Databricks notebook source
# MAGIC %md
# MAGIC # DS2B — source to Bronze
# MAGIC Configuration controls every source. Secrets are resolved at runtime from the configured secret scope.

# COMMAND ----------

import os
import sys
import uuid
from datetime import date
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "common_utils").is_dir():
        sys.path.insert(0, str(candidate))
        break
from common_utils.config import load_config, qualified
from common_utils.governance import apply_governance, bootstrap_namespace, tag_columns
from common_utils.quality import evaluate_rules, persist_results, raise_for_blocking_failures
from common_utils.sources import read_landed_raw
from pyspark.sql import functions as F

dbutils.widgets.text("config_path", "src/bronze/config/bronze.json")
dbutils.widgets.text("environment", "dev")
dbutils.widgets.text("catalog", "retaildataplatform")
dbutils.widgets.text("secret_scope", "retail-platform-dev")
dbutils.widgets.text("run_date", date.today().isoformat())

config = load_config(dbutils.widgets.get("config_path"))
catalog = dbutils.widgets.get("catalog") or config["platform"]["catalog"]
scope = dbutils.widgets.get("secret_scope")
platform = config["platform"]
schema = platform["schema"]
bootstrap_namespace(spark, catalog, schema, platform["raw_volume"])
run_id = str(uuid.uuid4())
volume_path = f"/Volumes/{catalog}/{schema}/{platform['raw_volume']}"

for source in config["sources"]:
    source_df, raw_path = read_landed_raw(spark, source, volume_path, dbutils.widgets.get("run_date"))
    bronze_df = (source_df
        .withColumn("last_update_ts", F.current_timestamp())
        .withColumn("file_path", F.lit(raw_path))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_source_system", F.lit(source["name"])))

    quality = evaluate_rules(bronze_df, source.get("quality_rules", []))
    persist_results(spark, catalog, schema, platform["quality_results_table"], source["target_table"], run_id, quality)
    raise_for_blocking_failures(quality)

    target = f"{catalog}.{schema}.{source['target_table']}"
    bronze_df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(target)
    apply_governance(spark, catalog, schema, source["target_table"], platform["tags"], platform.get("owner"))
    tag_columns(spark, catalog, schema, source["target_table"], source.get("pii_columns", []))

print(f"DS2B completed successfully. run_id={run_id}")
