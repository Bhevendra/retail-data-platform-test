from common_utils.config import qualified


def bootstrap_namespace(spark, catalog: str, schema: str, volume: str | None = None) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    if volume:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {qualified(catalog, schema, volume)}")


def apply_governance(spark, catalog: str, schema: str, object_name: str, tags: dict, owner: str | None = None) -> None:
    target = qualified(catalog, schema, object_name)
    for key, value in tags.items():
        spark.sql(f"ALTER TABLE {target} SET TAGS ('{key}' = '{value}')")
    if owner:
        spark.sql(f"ALTER TABLE {target} OWNER TO `{owner}`")


def tag_columns(spark, catalog: str, schema: str, object_name: str, columns: list[str], classification: str = "pii") -> None:
    """Apply Unity Catalog column tags for sensitive attributes."""
    target = qualified(catalog, schema, object_name)
    for column in columns:
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{column}` SET TAGS ('classification' = '{classification}')")
