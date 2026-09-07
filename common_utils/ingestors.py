"""Readers for common source systems, and helpers to land bytes in a volume.

Each reader takes plain arguments (host, table, credentials …) rather than a
config object, so they work on any project regardless of how it stores config.

All of these run on Databricks Serverless: Python clients only, no JVM
connectors, no ``SparkContext``.
"""

from __future__ import annotations

import io
from typing import Any
from urllib.parse import urlparse

from pyspark.sql import DataFrame

# --------------------------------------------------------------------------- #
# Relational
# --------------------------------------------------------------------------- #
SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def jdbc_url(host: str, database: str, port: int = 1433, **properties: str) -> str:
    """Build a SQL Server JDBC URL. Encryption is always on; other properties are yours.

    Databricks Serverless needs ``trustServerCertificate='true'`` and a generous
    ``loginTimeout`` to reach Azure SQL.
    """
    options = {"encrypt": "true", **properties}
    rendered = ";".join(f"{key}={value}" for key, value in options.items())
    return f"jdbc:sqlserver://{host}:{port};databaseName={database};{rendered}"


def read_jdbc(
    spark,
    url: str,
    user: str,
    password: str,
    table: str | None = None,
    query: str | None = None,
    driver: str = SQLSERVER_DRIVER,
    options: dict[str, str] | None = None,
) -> DataFrame:
    """Read a table or a query over JDBC."""
    if not table and not query:
        raise ValueError("read_jdbc needs either 'table' or 'query'")
    reader = spark.read.format("jdbc").option("url", url).option("user", user).option("password", password).option("driver", driver)
    reader = reader.option("query", query) if query else reader.option("dbtable", table)
    for key, value in (options or {}).items():
        reader = reader.option(key, value)
    return reader.load()


# --------------------------------------------------------------------------- #
# Document / NoSQL
# --------------------------------------------------------------------------- #
def read_mongo_collection(spark, connection_string: str, database: str, collection: str, batch_size: int = 1000) -> DataFrame:
    """Read a MongoDB / Cosmos DB (Mongo API) collection into a flat DataFrame.

    Nested documents and arrays become JSON strings so that heterogeneous shapes
    never break schema inference; parse them downstream with an explicit schema.
    """
    from bson import json_util
    from pymongo import MongoClient

    client = MongoClient(connection_string, serverSelectionTimeoutMS=30_000)
    try:
        documents = [flatten_document(doc, json_util) for doc in client[database][collection].find({}, batch_size=batch_size)]
    finally:
        client.close()
    if not documents:
        raise ValueError(f"Collection is empty: {database}.{collection}")
    schema = infer_flat_schema(documents)
    rows = [tuple(coerce_value(doc.get(name), dtype) for name, dtype in schema) for doc in documents]
    return spark.createDataFrame(rows, ", ".join(f"`{name}` {dtype}" for name, dtype in schema))


def flatten_document(document: dict, json_util) -> dict:
    """One level deep: keep scalars, JSON-encode dicts/lists, stringify everything else."""
    flat = {}
    for field, value in document.items():
        if isinstance(value, (dict, list)):
            flat[field] = json_util.dumps(value)
        elif value is None or isinstance(value, (str, int, float, bool)):
            flat[field] = value
        else:  # ObjectId, datetime, Decimal128 …
            flat[field] = str(value)
    return flat


def infer_flat_schema(rows: list[dict]) -> list[tuple[str, str]]:
    """Decide one Spark type per field. All-null and mixed-type fields become strings."""
    columns = sorted({key for row in rows for key in row})
    schema = []
    for column in columns:
        kinds = {type(row[column]) for row in rows if row.get(column) is not None}
        if kinds == {bool}:
            dtype = "boolean"
        elif kinds == {int}:
            dtype = "long"
        elif kinds and kinds <= {int, float}:
            dtype = "double"
        else:
            dtype = "string"
        schema.append((column, dtype))
    return schema


def coerce_value(value: Any, dtype: str) -> Any:
    if value is None:
        return None
    if dtype == "string":
        return value if isinstance(value, str) else str(value)
    if dtype == "double":
        return float(value)
    return value


# --------------------------------------------------------------------------- #
# Object storage
# --------------------------------------------------------------------------- #
def s3_client(access_key_id: str, secret_access_key: str, region: str):
    import boto3

    return boto3.client("s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key, region_name=region)


def list_s3_objects(client, uri: str) -> list[str]:
    """Return the object keys under an s3:// URI (a single object, or every object under a prefix)."""
    location = urlparse(uri)
    if location.scheme != "s3" or not location.netloc or not location.path:
        raise ValueError(f"Invalid S3 URI: {uri}")
    key = location.path.lstrip("/")
    if not key.endswith("/"):
        return [key]
    response = client.list_objects_v2(Bucket=location.netloc, Prefix=key)
    return [obj["Key"] for obj in response.get("Contents", []) if not obj["Key"].endswith("/")]


def copy_s3_to_volume(client, uri: str, target_folder: str, workspace_client=None) -> list[str]:
    """Copy S3 objects into a Unity Catalog volume byte-for-byte.

    Uses the Databricks Files API because Serverless will not copy from local
    ``/tmp`` into a volume. Returns the paths written.
    """
    from databricks.sdk import WorkspaceClient

    workspace = workspace_client or WorkspaceClient()
    bucket = urlparse(uri).netloc
    keys = list_s3_objects(client, uri)
    if not keys:
        raise FileNotFoundError(f"No S3 objects found at {uri}")
    workspace.files.create_directory(target_folder)
    written = []
    for key in keys:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        path = f"{target_folder}/{key.rsplit('/', 1)[-1]}"
        workspace.files.upload(path, io.BytesIO(body), overwrite=True)
        written.append(path)
    return written


# --------------------------------------------------------------------------- #
# Files
# --------------------------------------------------------------------------- #
CSV_DEFAULTS = {"header": "true", "escape": '"', "multiLine": "true"}


def read_files(spark, path: str, file_format: str, infer_schema: bool = True, options: dict[str, str] | None = None) -> DataFrame:
    """Read csv / json / parquet / delta files with sane defaults for messy CSV."""
    settings = dict(options or {})
    if file_format == "csv":
        settings = {**CSV_DEFAULTS, "inferSchema": str(infer_schema).lower(), **settings}
    reader = spark.read.format(file_format)
    for key, value in settings.items():
        reader = reader.option(key, value)
    return reader.load(path)


def write_files(df: DataFrame, path: str, file_format: str, mode: str = "overwrite", options: dict[str, str] | None = None) -> str:
    """Serialise a DataFrame to files (used when landing a relational or document extract)."""
    settings = dict(options or {})
    if file_format == "csv":
        settings = {"header": "true", "quoteAll": "true", "escape": '"', **settings}
    writer = df.write.mode(mode).format(file_format)
    for key, value in settings.items():
        writer = writer.option(key, value)
    writer.save(path)
    return path


def read_excel(spark, path: str, sheet_name: str | int = 0, header_row: int = 0):
    """Read an Excel sheet via pandas (Excel has no Spark reader on Serverless).

    Needs ``openpyxl``. Fine for reference data and small extracts; not for millions of rows.
    """
    import pandas as pd

    pandas_df = pd.read_excel(path, sheet_name=sheet_name, header=header_row, dtype=str)
    pandas_df.columns = [str(c).strip().replace(" ", "_") for c in pandas_df.columns]
    return spark.createDataFrame(pandas_df.astype(object).where(pandas_df.notna(), None))


def read_xml(spark, path: str, row_tag: str):
    """Read an XML file by parsing it in Python and building rows of strings.

    Avoids the spark-xml JVM package, which Serverless cannot install.
    """
    import xml.etree.ElementTree as ET

    tree = ET.parse(path)
    rows = []
    for element in tree.getroot().iter(row_tag):
        row = {child.tag: (child.text or "").strip() for child in element}
        row.update({key: value for key, value in element.attrib.items()})
        rows.append(row)
    if not rows:
        raise ValueError(f"No <{row_tag}> elements found in {path}")
    columns = sorted({key for row in rows for key in row})
    data = [tuple(row.get(column) for column in columns) for row in rows]
    return spark.createDataFrame(data, ", ".join(f"`{c}` string" for c in columns))
