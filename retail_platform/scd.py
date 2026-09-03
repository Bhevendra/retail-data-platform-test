"""Slowly changing dimension merges (type 1 and type 2) for Silver.

Both merges are deterministic and idempotent:

* the batch is de-duplicated on the business key (latest ``order_by`` wins);
* change detection uses ``_row_hash`` (sha2 over business columns, null-safe),
  so re-processing an identical batch is a no-op;
* ``MERGE ... WITH SCHEMA EVOLUTION`` lets Silver absorb *additive* source
  columns without manual DDL.

SCD2 tables carry ``effective_from``, ``effective_to`` and ``is_current``.
Optionally, keys missing from a full extract are closed (``detect_deletes``).
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from retail_platform.governance import table_exists

SCD2_COLUMNS = ["effective_from", "effective_to", "is_current"]
HASH_COLUMN = "_row_hash"
HIGH_DATE = "9999-12-31 00:00:00"


def row_hash(df: DataFrame, columns: list[str], output: str = HASH_COLUMN) -> DataFrame:
    """Null-safe, order-stable hash over the business columns."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in sorted(columns)]
    return df.withColumn(output, F.sha2(F.concat_ws("||", *parts), 256))


def deduplicate(df: DataFrame, keys: list[str], order_by: str) -> DataFrame:
    """Keep one row per key: the latest by ``order_by`` (ties broken by hash for determinism)."""
    ordering = [F.col(order_by).desc_nulls_last()] if order_by in df.columns else []
    if HASH_COLUMN in df.columns:
        ordering.append(F.col(HASH_COLUMN))
    if not ordering:
        return df.dropDuplicates(keys)
    window = Window.partitionBy(*keys).orderBy(*ordering)
    return df.withColumn("__rn", F.row_number().over(window)).filter("__rn = 1").drop("__rn")


def _join_condition(keys: list[str]) -> str:
    return " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)


def _batch_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def merge_type_1(spark, source_df: DataFrame, target: str, keys: list[str]) -> None:
    """Upsert: update rows whose hash changed, insert new keys."""
    if not _exists(spark, target):
        source_df.write.format("delta").saveAsTable(target)
        return
    view = _temp_view(source_df, "scd1_source")
    spark.sql(
        f"""
        MERGE WITH SCHEMA EVOLUTION INTO {target} t
        USING {view} s ON {_join_condition(keys)}
        WHEN MATCHED AND t.`{HASH_COLUMN}` <> s.`{HASH_COLUMN}` THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def merge_type_2(spark, source_df: DataFrame, target: str, keys: list[str], detect_deletes: bool = False) -> None:
    """History-preserving merge. New versions get effective_from = batch timestamp."""
    batch_ts = _batch_timestamp()
    versioned = (
        source_df.withColumn("effective_from", F.lit(batch_ts).cast("timestamp"))
        .withColumn("effective_to", F.lit(HIGH_DATE).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )
    if not _exists(spark, target):
        versioned.write.format("delta").saveAsTable(target)
        return

    view = _temp_view(versioned, "scd2_source")
    # 1) Close current versions whose content changed (or whose key vanished from a full extract).
    delete_clause = (
        f"WHEN NOT MATCHED BY SOURCE AND t.is_current = true THEN UPDATE SET is_current = false, effective_to = timestamp'{batch_ts}'"
        if detect_deletes
        else ""
    )
    spark.sql(
        f"""
        MERGE INTO {target} t
        USING {view} s ON {_join_condition(keys)} AND t.is_current = true
        WHEN MATCHED AND t.`{HASH_COLUMN}` <> s.`{HASH_COLUMN}`
          THEN UPDATE SET is_current = false, effective_to = timestamp'{batch_ts}'
        {delete_clause}
        """
    )
    # 2) Insert new versions for keys that are new or no longer have a current row with the same hash.
    spark.sql(
        f"""
        MERGE WITH SCHEMA EVOLUTION INTO {target} t
        USING {view} s ON {_join_condition(keys)} AND t.is_current = true AND t.`{HASH_COLUMN}` = s.`{HASH_COLUMN}`
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _exists(spark, target: str) -> bool:
    catalog, schema, table = [p.strip("`") for p in target.split(".")]
    return table_exists(spark, catalog, schema, table)


def _temp_view(df: DataFrame, name: str) -> str:
    view = f"_{name}"
    df.createOrReplaceTempView(view)
    return view
