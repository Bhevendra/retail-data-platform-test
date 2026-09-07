"""Small, composable cleaning helpers.

Each function does one thing and returns a DataFrame, so a Silver notebook reads
as a chain of named steps instead of a wall of ``withColumn``.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

#: Values that *look* like NULL but arrive as text.
NULL_LITERALS = ["", "NULL", "null", "Null", "N/A", "NA", "NA NA", "nan", "None"]

#: Integral targets need a two-step cast: '1.56E9' -> double -> long.
INTEGRAL_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "long", "short", "byte"}


def normalise_nulls(df: DataFrame, literals: list[str] | None = None, columns: list[str] | None = None) -> DataFrame:
    """Turn text NULLs into real NULLs. Defaults to every string column that is not an audit column."""
    values = literals if literals is not None else NULL_LITERALS
    targets = columns or [f.name for f in df.schema.fields if f.dataType.simpleString() == "string" and not f.name.startswith("_")]
    for column in targets:
        if column in df.columns:
            df = df.withColumn(column, F.when(F.trim(F.col(column)).isin(values), None).otherwise(F.col(column)))
    return df


def trim_columns(df: DataFrame, columns: list[str] | None = None) -> DataFrame:
    """Strip leading/trailing whitespace; also collapses runs of inner whitespace."""
    targets = columns or [f.name for f in df.schema.fields if f.dataType.simpleString() == "string" and not f.name.startswith("_")]
    for column in targets:
        if column in df.columns:
            df = df.withColumn(column, F.regexp_replace(F.trim(F.col(column)), r"\s+", " "))
    return df


def rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Rename by ``{old: new}``; missing columns are ignored so configs can be forward-looking."""
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def cast_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Cast by ``{column: type}`` using :func:`safe_cast` — a bad value becomes NULL, never an error."""
    for column, dtype in mapping.items():
        if column in df.columns:
            df = df.withColumn(column, safe_cast(column, dtype))
    return df


def safe_cast(column: str, dtype: str):
    """``try_cast`` that also accepts float-formatted integers ('1.564627663E9', '46506.0').

    Databricks runs SQL in ANSI mode, where a plain ``CAST`` of a bad value raises
    and kills the whole load. A transformation should never do that: produce NULL
    and let a quality rule decide whether NULL is acceptable.
    """
    target = dtype.strip().lower()
    if target in INTEGRAL_TYPES:
        return F.expr(f"try_cast(try_cast(`{column}` AS DOUBLE) AS {target})")
    return F.expr(f"try_cast(`{column}` AS {dtype})")


def strip_float_suffix(df: DataFrame, columns: list[str]) -> DataFrame:
    """Turn '46506.0' back into '46506' for identifiers that were once floats."""
    for column in columns:
        if column in df.columns:
            df = df.withColumn(column, F.regexp_replace(F.col(column), r"\.0$", ""))
    return df


def parse_json_column(df: DataFrame, column: str, schema: str) -> DataFrame:
    """Replace a JSON string column with a typed struct/array using a DDL schema string."""
    if column in df.columns:
        df = df.withColumn(column, F.from_json(F.col(column), schema))
    return df


def explode_array(df: DataFrame, column: str, alias: str = "element", position_column: str | None = None, keep_array: bool = False) -> DataFrame:
    """One row per array element — the way a nested structure becomes its own table.

    ``position_column`` gets a 1-based index (line numbers). Empty arrays produce
    no rows, which is what you want for order lines; use ``explode_outer`` yourself
    if a parent without children must survive.
    """
    if column not in df.columns:
        return df
    if position_column:
        df = df.select("*", F.posexplode(F.col(column)).alias("__pos", alias)).withColumn(position_column, F.col("__pos") + 1).drop("__pos")
    else:
        df = df.select("*", F.explode(F.col(column)).alias(alias))
    return df if keep_array else df.drop(column)


def add_derived(df: DataFrame, expressions: dict[str, str]) -> DataFrame:
    """Add columns from SQL expressions, in order (later ones can use earlier ones)."""
    for column, expression in expressions.items():
        df = df.withColumn(column, F.expr(expression))
    return df


def drop_columns(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.drop(*[c for c in columns if c in df.columns])
