"""Slowly changing dimension merges — the most reusable thing in this library.

Both merges are idempotent: the batch is de-duplicated on the business key, and
change detection uses a null-safe hash of the business columns, so re-processing
an identical batch does nothing at all.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HASH_COLUMN = "_row_hash"
EFFECTIVE_FROM = "effective_from"
EFFECTIVE_TO = "effective_to"
IS_CURRENT = "is_current"
SCD2_COLUMNS = [EFFECTIVE_FROM, EFFECTIVE_TO, IS_CURRENT]
HIGH_DATE = "9999-12-31 00:00:00"

SCD2_COLUMN_COMMENTS = {
    EFFECTIVE_FROM: "UTC timestamp from which this version is valid.",
    EFFECTIVE_TO: "UTC timestamp until which this version was valid; 9999-12-31 for the current version.",
    IS_CURRENT: "TRUE for the latest version of the business key.",
    HASH_COLUMN: "SHA-256 over the business columns; used to detect real changes.",
}


def row_hash(df: DataFrame, columns: list[str], output: str = HASH_COLUMN) -> DataFrame:
    """Null-safe, order-independent fingerprint of the business columns.

    Sorted so the hash does not depend on column order; ``∅`` so that NULL and
    the empty string hash differently.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in sorted(columns)]
    return df.withColumn(output, F.sha2(F.concat_ws("||", *parts), 256))


def business_columns(df: DataFrame, exclude: list[str] | None = None) -> list[str]:
    """Everything except audit/technical columns — the right input for :func:`row_hash`."""
    excluded = set(exclude or []) | set(SCD2_COLUMNS) | {HASH_COLUMN}
    return [c for c in df.columns if c not in excluded and not c.startswith("_")]


def deduplicate(df: DataFrame, keys: list[str], order_by: str | None = None) -> DataFrame:
    """One row per key: the latest by ``order_by``, ties broken by hash for determinism."""
    ordering = []
    if order_by and order_by in df.columns:
        ordering.append(F.col(order_by).desc_nulls_last())
    if HASH_COLUMN in df.columns:
        ordering.append(F.col(HASH_COLUMN))
    if not ordering:
        return df.dropDuplicates(keys)
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return df.withColumn("__rn", F.row_number().over(window)).filter("__rn = 1").drop("__rn")


def scd1_merge(spark, source_df: DataFrame, target: str, keys: list[str]) -> None:
    """Upsert: update rows whose hash changed, insert new keys, leave the rest alone."""
    if not _exists(spark, target):
        source_df.write.format("delta").saveAsTable(target)
        return
    view = _temp_view(source_df, "scd1_source")
    condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
    spark.sql(
        f"""
        MERGE WITH SCHEMA EVOLUTION INTO {target} t
        USING {view} s ON {condition}
        WHEN MATCHED AND t.`{HASH_COLUMN}` <> s.`{HASH_COLUMN}` THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def scd2_merge(spark, source_df: DataFrame, target: str, keys: list[str], detect_deletes: bool = False) -> None:
    """History-preserving merge: close changed versions, insert the new ones.

    Two statements, and the second one's join condition includes the hash — so a
    key whose current row was just closed is "not matched" and gets its new
    version inserted, while an unchanged key matches and is skipped.
    """
    batch_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    versioned = (
        source_df.withColumn(EFFECTIVE_FROM, F.lit(batch_ts).cast("timestamp"))
        .withColumn(EFFECTIVE_TO, F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn(IS_CURRENT, F.lit(True))
    )
    if not _exists(spark, target):
        versioned.write.format("delta").saveAsTable(target)
        return

    view = _temp_view(versioned, "scd2_source")
    condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in keys)
    delete_clause = (
        f"WHEN NOT MATCHED BY SOURCE AND t.{IS_CURRENT} = true THEN UPDATE SET {IS_CURRENT} = false, {EFFECTIVE_TO} = timestamp'{batch_ts}'"
        if detect_deletes
        else ""
    )
    spark.sql(
        f"""
        MERGE INTO {target} t USING {view} s ON {condition} AND t.{IS_CURRENT} = true
        WHEN MATCHED AND t.`{HASH_COLUMN}` <> s.`{HASH_COLUMN}`
          THEN UPDATE SET {IS_CURRENT} = false, {EFFECTIVE_TO} = timestamp'{batch_ts}'
        {delete_clause}
        """
    )
    spark.sql(
        f"""
        MERGE WITH SCHEMA EVOLUTION INTO {target} t
        USING {view} s ON {condition} AND t.{IS_CURRENT} = true AND t.`{HASH_COLUMN}` = s.`{HASH_COLUMN}`
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _exists(spark, target: str) -> bool:
    catalog, schema, table = (part.strip("`") for part in target.split("."))
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")


def _temp_view(df: DataFrame, name: str) -> str:
    view = f"_{name}"
    df.createOrReplaceTempView(view)
    return view
