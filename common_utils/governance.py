"""Unity Catalog governance applied uniformly to every table the platform creates.

* namespaces (catalog / schema / volume) with comments
* table and column comments (the data dictionary BI and AI tools read)
* tags (domain, layer, classification) and PII column tags
* informational PRIMARY KEY / FOREIGN KEY constraints (BI tools use them to
  auto-detect joins; the Databricks Assistant and Genie use them for SQL generation)
* Delta table properties (change data feed, deletion vectors, auto-optimise)
* liquid clustering on the columns queried most
* optional least-privilege grants

All statements are idempotent and safe to re-run on every load.
"""

from __future__ import annotations

from common_utils.config import ForeignKey, qualified
from common_utils.runtime import get_logger, log

DEFAULT_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def _best_effort(spark, statement: str, **ctx) -> None:
    """Run governance DDL that some runtimes reject; never fail a load over metadata."""
    try:
        spark.sql(statement)
    except Exception as exc:  # noqa: BLE001
        log(get_logger(), "governance statement not applied", level=30, error=str(exc).splitlines()[0][:200], **ctx)


def _sql_string(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def bootstrap_namespace(spark, catalog: str, schema: str, volume: str | None = None, comment: str | None = None) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    schema_comment = f" COMMENT '{_sql_string(comment)}'" if comment else ""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`{schema_comment}")
    if volume:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {qualified(catalog, schema, volume)} COMMENT 'Immutable raw landing zone: original source bytes partitioned by source and load date'")


def table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    return spark.catalog.tableExists(f"{catalog}.{schema}.{table}")


def set_table_properties(spark, catalog: str, schema: str, table: str, properties: dict[str, str] | None = None) -> None:
    props = {**DEFAULT_TABLE_PROPERTIES, **(properties or {})}
    rendered = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
    spark.sql(f"ALTER TABLE {qualified(catalog, schema, table)} SET TBLPROPERTIES ({rendered})")


def set_clustering(spark, catalog: str, schema: str, table: str, columns: list[str]) -> None:
    """Liquid clustering: cheaper than partitioning for evolving query patterns; works on Serverless."""
    if not columns:
        return
    cols = ", ".join(f"`{c}`" for c in columns)
    try:
        spark.sql(f"ALTER TABLE {qualified(catalog, schema, table)} CLUSTER BY ({cols})")
    except Exception as exc:  # noqa: BLE001 - clustering is an optimisation, never a reason to fail a load
        log(get_logger(), "liquid clustering not applied", level=30, table=table, error=str(exc))


def apply_comments(spark, catalog: str, schema: str, table: str, table_comment: str | None, column_comments: dict[str, str], is_view: bool = False) -> None:
    target = qualified(catalog, schema, table)
    if table_comment:  # COMMENT ON TABLE also applies to views in Databricks SQL
        spark.sql(f"COMMENT ON TABLE {target} IS '{_sql_string(table_comment)}'")
    if not column_comments:
        return
    existing = {f.name for f in spark.table(target).schema.fields}
    for column, comment in column_comments.items():
        if column not in existing:
            log(get_logger(), "column comment skipped: column not found", level=30, table=table, column=column)
            continue
        if is_view:  # ALTER VIEW ... ALTER COLUMN is not accepted by every runtime; COMMENT ON COLUMN is.
            _best_effort(spark, f"COMMENT ON COLUMN {target}.`{column}` IS '{_sql_string(comment)}'", table=table, column=column, what="view column comment")
        else:
            spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{column}` COMMENT '{_sql_string(comment)}'")


def apply_tags(spark, catalog: str, schema: str, table: str, tags: dict[str, str], is_view: bool = False) -> None:
    if not tags:
        return
    target = qualified(catalog, schema, table)
    rendered = ", ".join(f"'{_sql_string(k)}' = '{_sql_string(v)}'" for k, v in tags.items())
    spark.sql(f"ALTER {'VIEW' if is_view else 'TABLE'} {target} SET TAGS ({rendered})")


def tag_pii_columns(spark, catalog: str, schema: str, table: str, columns: list[str], classification: str = "pii", is_view: bool = False) -> None:
    if not columns:
        return
    target = qualified(catalog, schema, table)
    existing = {f.name for f in spark.table(target).schema.fields}
    for column in columns:
        if column not in existing:
            continue
        statement = f"ALTER {'VIEW' if is_view else 'TABLE'} {target} ALTER COLUMN `{column}` SET TAGS ('classification' = '{classification}')"
        if is_view:  # column tags on views are not supported on all runtimes; the underlying tables carry the tags
            _best_effort(spark, statement, table=table, column=column, what="view column tag")
        else:
            spark.sql(statement)


def set_owner(spark, catalog: str, schema: str, table: str, owner: str | None, is_view: bool = False) -> None:
    if owner:
        try:
            spark.sql(f"ALTER {'VIEW' if is_view else 'TABLE'} {qualified(catalog, schema, table)} OWNER TO `{owner}`")
        except Exception as exc:  # noqa: BLE001 - the deploy identity may not be allowed to transfer ownership
            log(get_logger(), "ownership not transferred", level=30, table=table, owner=owner, error=str(exc))


def apply_constraints(spark, catalog: str, schema: str, table: str, primary_key: list[str], foreign_keys: list[ForeignKey]) -> None:
    """Informational PK/FK constraints (Unity Catalog does not enforce them; the quality layer does)."""
    if not primary_key:
        return
    target = qualified(catalog, schema, table)
    for column in primary_key:
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{column}` SET NOT NULL")
    pk_name = f"pk_{table}"
    # CASCADE drops foreign keys that reference this key; facts are rebuilt after dims and re-add theirs.
    spark.sql(f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS {pk_name} CASCADE")
    cols = ", ".join(f"`{c}`" for c in primary_key)
    spark.sql(f"ALTER TABLE {target} ADD CONSTRAINT {pk_name} PRIMARY KEY ({cols})")
    for fk in foreign_keys:
        fk_name = f"fk_{table}_{fk.references}"
        spark.sql(f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS {fk_name}")
        fk_cols = ", ".join(f"`{c}`" for c in fk.columns)
        ref_cols = ", ".join(f"`{c}`" for c in fk.referenced_columns)
        spark.sql(f"ALTER TABLE {target} ADD CONSTRAINT {fk_name} FOREIGN KEY ({fk_cols}) REFERENCES {qualified(catalog, schema, fk.references)} ({ref_cols})")


def apply_grants(spark, catalog: str, schema: str, grants: dict[str, list[str]]) -> None:
    """Schema-level grants, e.g. {"SELECT": ["bi_readers", "ai_engineers"]}. Requires ownership."""
    for privilege, principals in grants.items():
        for principal in principals:
            try:
                spark.sql(f"GRANT {privilege} ON SCHEMA `{catalog}`.`{schema}` TO `{principal}`")
            except Exception as exc:  # noqa: BLE001
                log(get_logger(), "grant not applied", level=30, schema=schema, privilege=privilege, principal=principal, error=str(exc))


def govern_table(
    spark,
    catalog: str,
    schema: str,
    table: str,
    *,
    table_comment: str | None = None,
    column_comments: dict[str, str] | None = None,
    tags: dict[str, str] | None = None,
    pii_columns: list[str] | None = None,
    owner: str | None = None,
    cluster_by: list[str] | None = None,
    primary_key: list[str] | None = None,
    foreign_keys: list[ForeignKey] | None = None,
    properties: dict[str, str] | None = None,
) -> None:
    """Apply the full governance bundle to a Delta table (idempotent)."""
    set_table_properties(spark, catalog, schema, table, properties)
    set_clustering(spark, catalog, schema, table, cluster_by or [])
    apply_comments(spark, catalog, schema, table, table_comment, column_comments or {})
    apply_tags(spark, catalog, schema, table, tags or {})
    tag_pii_columns(spark, catalog, schema, table, pii_columns or [])
    apply_constraints(spark, catalog, schema, table, primary_key or [], foreign_keys or [])
    set_owner(spark, catalog, schema, table, owner)


def govern_view(spark, catalog: str, schema: str, view: str, *, comment: str | None, column_comments: dict[str, str] | None, tags: dict[str, str] | None, pii_columns: list[str] | None, owner: str | None) -> None:
    apply_comments(spark, catalog, schema, view, comment, column_comments or {}, is_view=True)
    apply_tags(spark, catalog, schema, view, tags or {}, is_view=True)
    tag_pii_columns(spark, catalog, schema, view, pii_columns or [], is_view=True)
    set_owner(spark, catalog, schema, view, owner, is_view=True)
