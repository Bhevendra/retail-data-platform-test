"""Silver -> Gold: star schema, serving views and the semantic layer.

Products are declared in ``src/config/gold.json`` and built in dependency
order (dimensions before facts). Supported product types:

``date_dimension``  generated calendar table (no source needed)
``table``           materialised Delta table from a SQL statement (dims, facts, aggregates)
``view``            logical view (e.g. "current customers", one-big-table for AI/Genie)
``metric_view``     Unity Catalog metric view (YAML) - governed measures for BI and Genie

SQL statements may use ``${catalog}``, ``${silver}`` and ``${gold}`` placeholders.
Materialised tables get PK/FK constraints, comments, tags and liquid clustering,
which is what BI tools (Power BI, Tableau) and AI tools (Genie, the Assistant)
read to understand the model.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common_utils.config import GoldConfig, GoldProduct, qualified
from common_utils.governance import govern_table, govern_view
from common_utils.quality import evaluate_rules, persist_results, raise_for_blocking_failures
from common_utils.runtime import RunContext, get_logger, log


@dataclass(frozen=True)
class GoldBuildResult:
    product: str
    type: str
    rows_written: int | None


def render_sql(sql: str, catalog: str, silver_schema: str, gold_schema: str) -> str:
    return sql.replace("${catalog}", catalog).replace("${silver}", silver_schema).replace("${gold}", gold_schema)


def date_dimension(spark, start_date: str, end_date: str) -> DataFrame:
    """Calendar dimension with an integer surrogate key (yyyymmdd) and the attributes BI users ask for."""
    df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date")
    return df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.concat(F.year("date"), F.lit("-Q"), F.quarter("date")).alias("year_quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.date_trunc("month", "date").cast("date").alias("first_day_of_month"),
        F.last_day("date").alias("last_day_of_month"),
    )


def build_product(spark, ctx: RunContext, config: GoldConfig, product: GoldProduct) -> GoldBuildResult:
    logger = get_logger()
    catalog, gold = ctx.catalog, config.gold_schema
    target = qualified(catalog, gold, product.name)
    tags = {**config.platform.tags, "layer": "gold", **product.tags}

    if product.type in {"table", "date_dimension"}:
        if product.type == "date_dimension":
            df = date_dimension(spark, product.start_date, product.end_date)
        else:
            df = spark.sql(render_sql(product.sql, catalog, config.silver_schema, gold))
        df = df.withColumn("_gold_updated_at", F.current_timestamp()).withColumn("_run_id", F.lit(ctx.run_id))
        results = evaluate_rules(df, product.quality_rules)
        persist_results(spark, ctx, catalog, config.platform.ops_schema, "gold", product.name, results)
        raise_for_blocking_failures(results, product.name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        govern_table(
            spark,
            catalog,
            gold,
            product.name,
            table_comment=product.description,
            column_comments={**product.column_comments, "_gold_updated_at": "UTC timestamp of the last Gold rebuild.", "_run_id": "Run that produced this table; join to ops.pipeline_runs."},
            tags=tags,
            pii_columns=product.pii_columns,
            owner=config.platform.owner,
            cluster_by=product.cluster_by or product.primary_key,
            primary_key=product.primary_key,
            foreign_keys=product.foreign_keys,
        )
        rows = spark.table(target).count()
        return GoldBuildResult(product.name, product.type, rows)

    if product.type == "view":
        spark.sql(f"CREATE OR REPLACE VIEW {target} AS {render_sql(product.sql, catalog, config.silver_schema, gold)}")
        govern_view(spark, catalog, gold, product.name, comment=product.description, column_comments=product.column_comments, tags=tags, pii_columns=product.pii_columns, owner=config.platform.owner)
        return GoldBuildResult(product.name, product.type, None)

    if product.type == "metric_view":
        yaml_body = render_sql(product.yaml, catalog, config.silver_schema, gold)
        try:
            spark.sql(f"CREATE OR REPLACE VIEW {target} WITH METRICS LANGUAGE YAML AS $$\n{yaml_body}\n$$")
            govern_view(spark, catalog, gold, product.name, comment=product.description, column_comments={}, tags=tags, pii_columns=[], owner=config.platform.owner)
        except Exception as exc:  # noqa: BLE001 - metric views are a preview feature in some workspaces
            if config.platform.tags.get("strict_metric_views", "false") == "true":
                raise
            log(logger, "metric view not created (feature unavailable?)", level=30, product=product.name, error=str(exc))
        return GoldBuildResult(product.name, product.type, None)

    raise ValueError(f"Unsupported gold product type: {product.type}")


def data_dictionary_markdown(bronze, silver, gold) -> str:
    """Render the configured data dictionary (used for docs/data-dictionary.md and shared with consumers)."""
    lines = ["# Data dictionary", "", "Generated from `src/config/*.json`. Regenerate with `python -m common_utils.gold`.", ""]
    lines += ["## Gold (serve here)", ""]
    for product in gold.ordered_products():
        lines.append(f"### `{gold.platform.catalog}.{gold.gold_schema}.{product.name}` ({product.type})")
        lines.append("")
        if product.description:
            lines.append(product.description)
            lines.append("")
        if product.primary_key:
            lines.append(f"Primary key: `{', '.join(product.primary_key)}`")
        for fk in product.foreign_keys:
            lines.append(f"Foreign key: `{', '.join(fk.columns)}` -> `{fk.references}({', '.join(fk.referenced_columns)})`")
        if product.column_comments:
            lines += ["", "| Column | Description |", "| --- | --- |"]
            lines += [f"| `{c}` | {d} |" for c, d in product.column_comments.items()]
        lines.append("")
    lines += ["## Silver (conformed)", ""]
    for entity in silver.entities:
        lines.append(f"### `{silver.platform.catalog}.{silver.silver_schema}.{entity.target_table}` (SCD type {entity.scd_type}, key `{', '.join(entity.primary_keys)}`)")
        lines.append("")
        if entity.description:
            lines += [entity.description, ""]
        if entity.column_comments:
            lines += ["| Column | Description |", "| --- | --- |"]
            lines += [f"| `{c}` | {d} |" for c, d in entity.column_comments.items()]
        lines.append("")
    lines += ["## Bronze (raw, audit only)", ""]
    for source in bronze.sources:
        lines.append(f"- `{bronze.platform.catalog}.{bronze.schema}.{source.target_table}` <- {source.type} `{source.name}`; PII columns: {', '.join(source.pii_columns) or 'none'}")
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":  # pragma: no cover - CLI helper
    from pathlib import Path

    from common_utils.config import load_bronze_config, load_gold_config, load_silver_config, repo_root

    out = repo_root() / "docs" / "data-dictionary.md"
    Path(out).write_text(data_dictionary_markdown(load_bronze_config(), load_silver_config(), load_gold_config()), encoding="utf-8")
    print(f"wrote {out}")
