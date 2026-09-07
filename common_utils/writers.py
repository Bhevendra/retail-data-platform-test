"""Delta write patterns.

The important one is :func:`write_idempotent`: re-running a load date replaces
exactly that date's rows and nothing else, so retries and backfills are safe.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

DEFAULT_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def qualified(catalog: str, schema: str, table: str) -> str:
    """Back-quoted three-part name, safe for any identifier."""
    return f"`{catalog}`.`{schema}`.`{table}`"


def table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")


def create_namespace(spark, catalog: str, schema: str, volume: str | None = None, comment: str | None = None) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    suffix = f" COMMENT '{comment}'" if comment else ""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`{suffix}")
    if volume:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {qualified(catalog, schema, volume)}")


def write_idempotent(spark, df: DataFrame, catalog: str, schema: str, table: str, load_date: str, partition_column: str = "_load_date") -> None:
    """Replace only the rows of ``load_date``; create the table on first run.

    New source columns are absorbed (``mergeSchema``); a changed *type* still
    fails, which is what you want — that is a conversation with the source team.
    """
    target = qualified(catalog, schema, table)
    if not table_exists(spark, catalog, schema, table):
        df.write.format("delta").mode("overwrite").saveAsTable(target)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"{partition_column} = '{load_date}'")
        .option("mergeSchema", "true")
        .saveAsTable(target)
    )


def overwrite_table(spark, df: DataFrame, catalog: str, schema: str, table: str) -> None:
    """Full rebuild — for derived tables that can always be recomputed from their source."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(qualified(catalog, schema, table))


def set_table_properties(spark, catalog: str, schema: str, table: str, properties: dict[str, str] | None = None) -> None:
    rendered = ", ".join(f"'{k}' = '{v}'" for k, v in {**DEFAULT_PROPERTIES, **(properties or {})}.items())
    spark.sql(f"ALTER TABLE {qualified(catalog, schema, table)} SET TBLPROPERTIES ({rendered})")


def cluster_by(spark, catalog: str, schema: str, table: str, columns: list[str]) -> None:
    """Liquid clustering — cheaper than partitioning when query patterns change."""
    if not columns:
        return
    rendered = ", ".join(f"`{c}`" for c in columns)
    spark.sql(f"ALTER TABLE {qualified(catalog, schema, table)} CLUSTER BY ({rendered})")
