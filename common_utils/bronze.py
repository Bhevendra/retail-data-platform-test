"""Raw volume -> Bronze Delta tables.

Bronze keeps every load, unchanged apart from the audit columns below, so any
Silver/Gold state can be rebuilt. Loads are **idempotent per run_date**: the
write replaces only the rows of the same ``_load_date`` (Delta ``replaceWhere``),
so re-running a day never duplicates data and never touches other days.

Audit columns (prefixed ``_`` so they never clash with source columns):
``_load_date``, ``_ingested_at``, ``_run_id``, ``_source_system``,
``_source_file``, plus ``last_update_ts`` / ``file_path`` kept for backwards
compatibility with early consumers.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common_utils.config import BronzeConfig, Source, qualified
from common_utils.governance import govern_table, table_exists
from common_utils.quality import evaluate_rules, persist_results, raise_for_blocking_failures, split_quarantine
from common_utils.runtime import RunContext, get_logger, log
from common_utils.sources import read_landed_raw

AUDIT_COLUMNS = ["_load_date", "_ingested_at", "_run_id", "_source_system", "_source_file", "last_update_ts", "file_path"]
QUARANTINE_SUFFIX = "_quarantine"


@dataclass(frozen=True)
class BronzeLoadResult:
    entity: str
    rows_read: int
    rows_written: int
    rows_quarantined: int


def with_audit_columns(df: DataFrame, ctx: RunContext, source: Source, raw_path: str) -> DataFrame:
    if "_source_file" not in df.columns:
        df = df.withColumn("_source_file", F.lit(raw_path))
    return (
        df.withColumn("_load_date", F.lit(ctx.run_date_iso).cast("date"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(ctx.run_id))
        .withColumn("_source_system", F.lit(source.name))
        .withColumn("last_update_ts", F.current_timestamp())
        .withColumn("file_path", F.lit(raw_path))
    )


def write_idempotent(spark, df: DataFrame, catalog: str, schema: str, table: str, load_date: str) -> None:
    """Replace only this load date's rows. Additive schema evolution is allowed; type changes fail loudly."""
    target = qualified(catalog, schema, table)
    if not table_exists(spark, catalog, schema, table):
        df.write.format("delta").mode("overwrite").saveAsTable(target)
        return
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_load_date = '{load_date}'")
        .option("mergeSchema", "true")
        .saveAsTable(target)
    )


def load_source_to_bronze(spark, dbutils, ctx: RunContext, config: BronzeConfig, source: Source, volume_path: str) -> BronzeLoadResult:
    logger = get_logger()
    catalog, schema = ctx.catalog, config.schema
    raw_df, raw_path = read_landed_raw(spark, dbutils, source, volume_path, ctx.run_date_iso)
    bronze_df = with_audit_columns(raw_df, ctx, source, raw_path)  # no .cache(): PERSIST is not supported on Serverless
    rows_read = bronze_df.count()

    results = evaluate_rules(bronze_df, source.quality_rules)
    persist_results(spark, ctx, catalog, config.platform.ops_schema, "bronze", source.target_table, results)
    for result in results:
        log(logger, "quality rule evaluated", entity=source.target_table, rule=result.rule.name, severity=result.rule.severity, failed_rows=result.failed_rows, passed=result.passed)

    rows_quarantined = 0
    if source.on_quality_failure == "fail":
        raise_for_blocking_failures(results, source.target_table)
        valid_df = bronze_df
    elif source.on_quality_failure == "quarantine":
        valid_df, quarantined_df = split_quarantine(bronze_df, source.quality_rules)
        rows_quarantined = quarantined_df.count()
        if rows_quarantined:
            write_idempotent(spark, quarantined_df, catalog, schema, f"{source.target_table}{QUARANTINE_SUFFIX}", ctx.run_date_iso)
            log(logger, "rows quarantined", level=30, entity=source.target_table, rows=rows_quarantined)
        # Dataset-level error rules (unique / min_row_count) cannot be quarantined row by row: they still block.
        raise_for_blocking_failures([r for r in results if r.rule.type in {"unique", "min_row_count"}], source.target_table)
    else:  # warn
        valid_df = bronze_df
        for result in results:
            if not result.passed:
                log(logger, "quality rule failed (warn mode)", level=30, entity=source.target_table, rule=result.rule.name, failed_rows=result.failed_rows)

    write_idempotent(spark, valid_df, catalog, schema, source.target_table, ctx.run_date_iso)
    govern_table(
        spark,
        catalog,
        schema,
        source.target_table,
        table_comment=source.description or f"Bronze copy of {source.name}",
        column_comments={**_audit_comments(), **source.column_comments},
        tags={**config.platform.tags, "layer": "bronze", "source_system": source.name},
        pii_columns=source.pii_columns,
        owner=config.platform.owner,
        cluster_by=["_load_date", *source.primary_keys],
    )
    rows_written = rows_read - rows_quarantined
    return BronzeLoadResult(source.target_table, rows_read, rows_written, rows_quarantined)


def _audit_comments() -> dict[str, str]:
    return {
        "_load_date": "Logical load date (run_date parameter). Re-running a date replaces its rows.",
        "_ingested_at": "UTC timestamp when the row was written to Bronze.",
        "_run_id": "Pipeline run identifier; join to ops.pipeline_runs and ops.data_quality_results.",
        "_source_system": "Configured source name the row came from.",
        "_source_file": "Raw-volume file the row was read from (lineage to original bytes).",
        "last_update_ts": "Deprecated alias of _ingested_at kept for early consumers.",
        "file_path": "Deprecated alias of the raw landing folder kept for early consumers.",
    }
