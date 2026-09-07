"""Audit / lineage columns.

Keep the set small. Every column here has to answer a question somebody actually
asks; anything else is noise that ships with every row forever.

    _load_date    which logical day's extract this row belongs to  -> makes reloads idempotent
    _ingested_at  when it was written                              -> "when did this land?"
    _source_file  the file it was read from                        -> "where did this row come from?"
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOAD_DATE = "_load_date"
INGESTED_AT = "_ingested_at"
SOURCE_FILE = "_source_file"
AUDIT_COLUMNS = [LOAD_DATE, INGESTED_AT, SOURCE_FILE]

AUDIT_COLUMN_COMMENTS = {
    LOAD_DATE: "Logical load date of the extract. Re-running a date replaces exactly that date's rows.",
    INGESTED_AT: "UTC timestamp when the row was written to this table.",
    SOURCE_FILE: "Raw file the row was read from (lineage back to the landed bytes).",
}


def add_metadata_columns(df: DataFrame, load_date: str, source_file: str | None = None) -> DataFrame:
    """Add the three audit columns. ``source_file`` defaults to Spark's own file metadata."""
    if SOURCE_FILE not in df.columns:
        file_column = F.lit(source_file) if source_file else F.col("_metadata.file_path")
        df = df.withColumn(SOURCE_FILE, file_column)
    df = df.withColumn(LOAD_DATE, F.lit(load_date).cast("date")).withColumn(INGESTED_AT, F.current_timestamp())
    business = [c for c in df.columns if c not in AUDIT_COLUMNS]
    return df.select(*business, *AUDIT_COLUMNS)  # audit columns last, always in the same order


def drop_metadata_columns(df: DataFrame) -> DataFrame:
    """Remove audit columns (useful when a downstream layer re-stamps its own)."""
    return df.drop(*[c for c in AUDIT_COLUMNS if c in df.columns])
