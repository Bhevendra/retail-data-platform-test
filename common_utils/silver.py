"""Bronze -> Silver: standardise, validate and merge.

Silver is the *conformed* layer: typed columns, business-friendly names,
parsed nested structures, one row per business key (SCD1) or a full history
(SCD2). All shaping is declared in ``src/config/silver.json`` under
``transformations`` so it is reviewable in a pull request and unit-testable
without Databricks.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common_utils.bronze import AUDIT_COLUMNS
from common_utils.config import SilverConfig, SilverEntity, Transformations, qualified
from common_utils.governance import govern_table
from common_utils.quality import evaluate_rules, persist_results, raise_for_blocking_failures
from common_utils.runtime import RunContext, get_logger, log
from common_utils.scd import HASH_COLUMN, SCD2_COLUMNS, deduplicate, merge_type_1, merge_type_2, row_hash

LINEAGE_COLUMNS = ["_run_id", "_load_date", "_silver_updated_at"]


@dataclass(frozen=True)
class SilverLoadResult:
    entity: str
    rows_read: int
    rows_written: int


def apply_transformations(df: DataFrame, t: Transformations) -> DataFrame:
    """Pure function: apply configured shaping in a fixed, documented order.

    null literals -> trim -> rename -> cast -> parse_json -> explode -> derived -> filter -> drop.
    """
    # 1. Normalise null literals in string columns ("NULL", "", "N/A" ...) before casting.
    if t.null_literals:
        for field in df.schema.fields:
            if field.dataType.simpleString() == "string" and not field.name.startswith("_"):
                df = df.withColumn(field.name, F.when(F.trim(F.col(field.name)).isin(t.null_literals), None).otherwise(F.col(field.name)))
    # 2. Trim.
    for column in t.trim:
        if column in df.columns:
            df = df.withColumn(column, F.trim(F.col(column)))
    # 3. Rename to business names.
    for old, new in t.rename.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    # 4. Cast (tolerant: ANSI mode on Databricks makes a plain CAST fail the whole load on one bad value).
    for column, spark_type in t.cast.items():
        if column in df.columns:
            df = df.withColumn(column, tolerant_cast(column, spark_type))
    # 5. Parse JSON strings into typed structures.
    for column, ddl in t.parse_json.items():
        if column in df.columns:
            df = df.withColumn(column, F.from_json(F.col(column), ddl))
    # 6. Explode an array into rows (one child row per element; the element is addressable as <alias>.<field>).
    if t.explode and t.explode.column in df.columns:
        e = t.explode
        if e.position_column:
            exploded = F.posexplode(F.col(e.column)).alias("__pos", e.alias)
            df = df.select("*", exploded).withColumn(e.position_column, F.col("__pos") + 1).drop("__pos")
        else:
            df = df.select("*", F.explode(F.col(e.column)).alias(e.alias))
        if not e.keep_array:
            df = df.drop(e.column)
    # 7. Derived columns (SQL expressions can reference renamed/cast/parsed/exploded columns).
    for column, expression in t.derived.items():
        df = df.withColumn(column, F.expr(expression))
    # 8. Filter and drop.
    if t.filter:
        df = df.filter(t.filter)
    if t.drop:
        df = df.drop(*[c for c in t.drop if c in df.columns])
    if t.explode and t.explode.alias in df.columns:
        df = df.drop(t.explode.alias)  # the struct itself is scaffolding; derived columns carry its fields
    return df


_INTEGRAL_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "long", "short", "byte"}


def tolerant_cast(column: str, spark_type: str):
    """try_cast that also accepts float-formatted integers ('1.564627663E9', '46506.0') for integral targets.

    Bad values become NULL instead of failing the load; the entity's quality rules decide whether that blocks.
    """
    target = spark_type.strip().lower()
    if target in _INTEGRAL_TYPES:
        return F.expr(f"try_cast(try_cast(`{column}` AS DOUBLE) AS {target})")
    return F.expr(f"try_cast(`{column}` AS {spark_type})")


def business_columns(df: DataFrame, entity: SilverEntity) -> list[str]:
    excluded = set(entity.exclude_from_hash) | set(AUDIT_COLUMNS) | set(LINEAGE_COLUMNS) | set(SCD2_COLUMNS) | {HASH_COLUMN}
    return [c for c in df.columns if c not in excluded]


def prepare(df: DataFrame, entity: SilverEntity, ctx: RunContext) -> DataFrame:
    """Transform, hash, de-duplicate and attach lineage. Pure apart from the timestamp."""
    shaped = apply_transformations(df, entity.transformations)
    missing = [k for k in entity.primary_keys if k not in shaped.columns]
    if missing:
        raise ValueError(f"{entity.target_table}: primary key columns missing after transformations: {missing}")
    hashed = row_hash(shaped, business_columns(shaped, entity))
    deduped = deduplicate(hashed, entity.primary_keys, entity.order_by)
    keep = [c for c in deduped.columns if c not in AUDIT_COLUMNS or c in ("_run_id", "_load_date")]
    return (
        deduped.select(*keep)
        .withColumn("_run_id", F.lit(ctx.run_id))
        .withColumn("_load_date", F.lit(ctx.run_date_iso).cast("date"))
        .withColumn("_silver_updated_at", F.current_timestamp())
    )


def load_entity(spark, ctx: RunContext, config: SilverConfig, entity: SilverEntity, detect_deletes: bool = False) -> SilverLoadResult:
    logger = get_logger()
    catalog = ctx.catalog
    bronze = spark.table(qualified(catalog, config.bronze_schema, entity.source_table))
    # Only the rows of this load date are processed: Bronze is idempotent per date, so is Silver.
    batch = bronze.filter(F.col("_load_date") == F.lit(ctx.run_date_iso).cast("date"))
    prepared = prepare(batch, entity, ctx)  # no .cache(): PERSIST is not supported on Serverless
    rows_read = prepared.count()
    if rows_read == 0:
        log(logger, "no rows for run_date; silver entity skipped", level=30, entity=entity.target_table, run_date=ctx.run_date_iso)
        return SilverLoadResult(entity.target_table, 0, 0)

    results = evaluate_rules(prepared, entity.quality_rules)
    persist_results(spark, ctx, catalog, config.platform.ops_schema, "silver", entity.target_table, results)
    raise_for_blocking_failures(results, entity.target_table)

    target = qualified(catalog, config.silver_schema, entity.target_table)
    if entity.scd_type == 1:
        merge_type_1(spark, prepared, target, entity.primary_keys)
    else:
        merge_type_2(spark, prepared, target, entity.primary_keys, detect_deletes=detect_deletes)

    comments = {**entity.column_comments, **_lineage_comments(entity.scd_type)}
    govern_table(
        spark,
        catalog,
        config.silver_schema,
        entity.target_table,
        table_comment=entity.description or f"Conformed {entity.target_table} (SCD type {entity.scd_type})",
        column_comments=comments,
        tags={**config.platform.tags, "layer": "silver", "scd_type": str(entity.scd_type)},
        pii_columns=entity.pii_columns,
        owner=config.platform.owner,
        cluster_by=entity.cluster_by,
    )
    return SilverLoadResult(entity.target_table, rows_read, rows_read)


def _lineage_comments(scd_type: int) -> dict[str, str]:
    comments = {
        HASH_COLUMN: "SHA-256 over business columns; used for change detection.",
        "_run_id": "Run that last wrote this version; join to ops.pipeline_runs.",
        "_load_date": "Bronze load date the version came from.",
        "_silver_updated_at": "UTC timestamp when this version was written to Silver.",
    }
    if scd_type == 2:
        comments.update(
            {
                "effective_from": "UTC timestamp from which this version is valid.",
                "effective_to": "UTC timestamp until which this version was valid; 9999-12-31 for the current version.",
                "is_current": "TRUE for the latest version of the business key. Filter on it for point-in-time = now.",
            }
        )
    return comments
