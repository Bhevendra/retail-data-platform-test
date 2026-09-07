"""Unity Catalog metadata: comments, tags, constraints, ownership, grants.

All statements are idempotent, and anything a runtime might reject (tags on
views, ownership transfer, grants to groups that do not exist) is applied
best-effort: metadata must never fail a data load.
"""

from __future__ import annotations

from common_utils.logger import get_logger, log_warning

logger = get_logger("governance")


def sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def qualified(catalog: str, schema: str, name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


def _try(spark, statement: str, **context) -> bool:
    try:
        spark.sql(statement)
        return True
    except Exception as exc:  # noqa: BLE001 - metadata is never worth failing a run for
        log_warning(logger, "governance statement skipped", error=str(exc).splitlines()[0][:200], **context)
        return False


def set_table_comment(spark, target: str, comment: str) -> None:
    """``COMMENT ON TABLE`` also works for views in Databricks SQL."""
    if comment:
        _try(spark, f"COMMENT ON TABLE {target} IS '{sql_literal(comment)}'", target=target)


def set_column_comments(spark, target: str, comments: dict[str, str], is_view: bool = False) -> None:
    if not comments:
        return
    for column, comment in comments.items():
        statement = (
            f"COMMENT ON COLUMN {target}.`{column}` IS '{sql_literal(comment)}'"
            if is_view
            else f"ALTER TABLE {target} ALTER COLUMN `{column}` COMMENT '{sql_literal(comment)}'"
        )
        _try(spark, statement, target=target, column=column)


def set_tags(spark, target: str, tags: dict[str, str], is_view: bool = False) -> None:
    if not tags:
        return
    rendered = ", ".join(f"'{sql_literal(k)}' = '{sql_literal(v)}'" for k, v in tags.items())
    _try(spark, f"ALTER {'VIEW' if is_view else 'TABLE'} {target} SET TAGS ({rendered})", target=target)


def tag_columns(spark, target: str, columns: list[str], tag: str = "classification", value: str = "pii", is_view: bool = False) -> None:
    """Tag sensitive columns so discovery, masking and audits can find them."""
    for column in columns or []:
        _try(
            spark,
            f"ALTER {'VIEW' if is_view else 'TABLE'} {target} ALTER COLUMN `{column}` SET TAGS ('{tag}' = '{value}')",
            target=target,
            column=column,
        )


def set_primary_key(spark, target: str, columns: list[str], name: str) -> None:
    """Informational PK — Unity Catalog does not enforce it, but BI tools and Genie read it."""
    if not columns:
        return
    for column in columns:
        _try(spark, f"ALTER TABLE {target} ALTER COLUMN `{column}` SET NOT NULL", target=target, column=column)
    _try(spark, f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS {name} CASCADE", target=target)
    rendered = ", ".join(f"`{c}`" for c in columns)
    _try(spark, f"ALTER TABLE {target} ADD CONSTRAINT {name} PRIMARY KEY ({rendered})", target=target)


def set_foreign_key(spark, target: str, name: str, columns: list[str], references: str, referenced_columns: list[str]) -> None:
    _try(spark, f"ALTER TABLE {target} DROP CONSTRAINT IF EXISTS {name}", target=target)
    cols = ", ".join(f"`{c}`" for c in columns)
    ref_cols = ", ".join(f"`{c}`" for c in referenced_columns)
    _try(spark, f"ALTER TABLE {target} ADD CONSTRAINT {name} FOREIGN KEY ({cols}) REFERENCES {references} ({ref_cols})", target=target)


def set_owner(spark, target: str, owner: str | None, is_view: bool = False) -> None:
    if owner:
        _try(spark, f"ALTER {'VIEW' if is_view else 'TABLE'} {target} OWNER TO `{owner}`", target=target)


def grant_on_schema(spark, catalog: str, schema: str, grants: dict[str, list[str]]) -> None:
    """``{"SELECT": ["bi_readers", "ai_engineers"]}`` — least privilege, declared in config."""
    for privilege, principals in (grants or {}).items():
        for principal in principals:
            _try(spark, f"GRANT {privilege} ON SCHEMA `{catalog}`.`{schema}` TO `{principal}`", schema=schema, principal=principal)
