# Databricks notebook source
# MAGIC %md
# MAGIC # S2G — Silver -> Gold
# MAGIC Builds the star schema (dimensions, facts), serving views and the semantic layer
# MAGIC (metric views) declared in `src/config/gold.json`, in dependency order, then
# MAGIC applies constraints, comments, tags and grants so BI and AI consumers get a
# MAGIC self-describing model.

# COMMAND ----------

import sys
from datetime import date
from pathlib import Path

for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "retail_platform").is_dir():
        sys.path.insert(0, str(candidate))
        break

from retail_platform.config import load_gold_config
from retail_platform.gold import build_product
from retail_platform.governance import apply_grants, bootstrap_namespace
from retail_platform.observability import ensure_ops_schema, track_entity
from retail_platform.quality import ensure_results_table
from retail_platform.runtime import get_logger, log, widget_context

# COMMAND ----------

dbutils.widgets.text("config_path", "src/config/gold.json")
dbutils.widgets.text("products", "")  # optional comma-separated subset (dependencies are NOT rebuilt automatically)
ctx = widget_context(
    dbutils,
    task="s2g",
    defaults={"environment": "dev", "catalog": "retaildataplatform", "secret_scope": "retail-platform-dev", "run_date": date.today().isoformat()},
)
config = load_gold_config(dbutils.widgets.get("config_path"))
selected = {s.strip() for s in dbutils.widgets.get("products").split(",") if s.strip()}
products = [p for p in config.ordered_products() if not selected or p.name in selected]
logger = get_logger()

# COMMAND ----------

bootstrap_namespace(spark, ctx.catalog, config.gold_schema, comment="Gold: star schema, serving views and governed metrics for BI and AI")
ensure_ops_schema(spark, ctx.catalog, config.platform.ops_schema)
ensure_results_table(spark, ctx.catalog, config.platform.ops_schema)

# Gold products depend on each other, so the first failure stops the build (partial stars mislead consumers).
for product in products:
    with track_entity(spark, ctx, ctx.catalog, config.platform.ops_schema, layer="gold", entity=product.name) as run:
        result = build_product(spark, ctx, config, product)
        run.rows_written = result.rows_written
        log(logger, "gold product built", product=result.product, type=result.type, rows=result.rows_written)

apply_grants(spark, ctx.catalog, config.gold_schema, config.platform.grants)
log(logger, "S2G completed", products=[p.name for p in products], **ctx.as_dict())
