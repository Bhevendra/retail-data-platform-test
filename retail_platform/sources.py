"""Source connectors and raw-volume landing.

Design rules
------------
* The raw volume is immutable and keeps the *original* representation of the
  data (S3 objects are copied byte-for-byte; document and relational sources
  are serialised once, as JSON / CSV, at extraction time).
* Landing is idempotent per ``run_date``: re-running a day replaces that day's
  landing folder, nothing else.
* Everything works on Databricks Serverless: Python clients are installed with
  pip, and no ``spark._jvm`` / ``SparkContext`` access is used.
"""

from __future__ import annotations

import io
from urllib.parse import urlparse

from pyspark.sql import functions as F

from retail_platform.config import Source
from retail_platform.runtime import get_logger, log

_LANDING_FOLDER = "load_date={run_date}"


def landing_path(volume_path: str, source: Source, run_date: str) -> str:
    return f"{volume_path}/{source.name}/{_LANDING_FOLDER.format(run_date=run_date)}"


def _secret(dbutils, scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


# --------------------------------------------------------------------------- #
# Readers
# --------------------------------------------------------------------------- #
def jdbc_url(source: Source) -> str:
    """Build the SQL Server JDBC URL. Encryption is mandatory; other driver properties come from config."""
    options = source.options
    jdbc_options = {"encrypt": "true", **options.get("jdbc_options", {})}
    properties = ";".join(f"{k}={v}" for k, v in jdbc_options.items())
    return f"jdbc:sqlserver://{options['host']}:{options.get('port', 1433)};databaseName={options['database']};{properties}"


def read_sqlserver(spark, dbutils, source: Source, secret_scope: str):
    keys = source.secret_keys
    reader = (
        spark.read.format("jdbc")
        .option("url", jdbc_url(source))
        .option("user", _secret(dbutils, secret_scope, keys["username"]))
        .option("password", _secret(dbutils, secret_scope, keys["password"]))
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    )
    query = source.options.get("query")
    if query:
        reader = reader.option("query", query)
    else:
        reader = reader.option("dbtable", source.options["table"])
    for key, value in source.read_options.items():
        reader = reader.option(key, value)
    return reader.load()


def read_cosmos(spark, dbutils, source: Source, secret_scope: str):
    """Read a Cosmos DB (Mongo API) collection with pymongo and return a DataFrame of JSON-serialised fields.

    Nested documents/arrays are serialised as JSON strings so heterogeneous
    shapes never break schema inference; Silver parses them with an explicit
    schema (``transformations.parse_json``).
    """
    from bson import json_util
    from pymongo import MongoClient

    connection = _secret(dbutils, secret_scope, source.secret_keys["connection_string"])
    batch_size = int(source.read_options.get("batch_size", 1000))
    client = MongoClient(connection, serverSelectionTimeoutMS=30_000)
    try:
        collection = client[source.options["database"]][source.options["collection"]]
        rows = [_flatten_document(document, json_util) for document in collection.find({}, batch_size=batch_size)]
    finally:
        client.close()
    if not rows:
        raise ValueError(f"Cosmos collection is empty: {source.options['database']}.{source.options['collection']}")
    schema = infer_schema(rows)
    normalised = [tuple(_coerce(row.get(name), dtype) for name, dtype in schema) for row in rows]
    return spark.createDataFrame(normalised, ", ".join(f"`{name}` {dtype}" for name, dtype in schema))


def infer_schema(rows: list[dict]) -> list[tuple[str, str]]:
    """Infer a flat Spark schema from Python rows; all-null and mixed-type fields become strings."""
    columns = sorted({key for row in rows for key in row})
    schema = []
    for column in columns:
        kinds = {type(row[column]) for row in rows if row.get(column) is not None}
        if kinds == {bool}:
            dtype = "boolean"
        elif kinds == {int}:
            dtype = "long"
        elif kinds <= {int, float} and kinds:
            dtype = "double"
        else:
            dtype = "string"
        schema.append((column, dtype))
    return schema


def _coerce(value, dtype: str):
    if value is None:
        return None
    if dtype == "string":
        return value if isinstance(value, str) else str(value)
    if dtype == "double":
        return float(value)
    return value


def _flatten_document(document: dict, json_util) -> dict:
    out = {}
    for field, value in document.items():
        if isinstance(value, (dict, list)):
            out[field] = json_util.dumps(value)
        elif value is None or isinstance(value, (str, int, float, bool)):
            out[field] = value
        else:  # ObjectId, datetime, Decimal128 ...
            out[field] = str(value)
    return out


# --------------------------------------------------------------------------- #
# Landing
# --------------------------------------------------------------------------- #
def land_dataframe(df, source: Source, target: str) -> str:
    """Serialise an extracted DataFrame into the raw volume (JSON for documents, CSV for relational)."""
    writer = df.write.mode("overwrite")
    if source.format == "json":
        writer.format("json").save(target)
    elif source.format == "csv":
        writer.option("header", "true").option("quoteAll", "true").option("escape", '"').csv(target)
    elif source.format == "parquet":
        writer.parquet(target)
    else:
        raise ValueError(f"Unsupported landing format: {source.format}")
    return target


def land_s3_object(dbutils, source: Source, secret_scope: str, target: str) -> str:
    """Copy the original S3 object(s) unchanged into the raw volume using scoped credentials.

    Uses the Databricks Files API instead of ``dbutils.fs.cp`` from ``/tmp``,
    which Serverless does not allow. Supports a single object or a prefix.
    """
    import boto3
    from databricks.sdk import WorkspaceClient

    location = urlparse(source.options["path"])
    if location.scheme != "s3" or not location.netloc or not location.path:
        raise ValueError(f"Invalid S3 URI: {source.options['path']}")
    keys = source.secret_keys
    client = boto3.client(
        "s3",
        aws_access_key_id=_secret(dbutils, secret_scope, keys["access_key_id"]),
        aws_secret_access_key=_secret(dbutils, secret_scope, keys["secret_access_key"]),
        region_name=source.options["region"],
    )
    workspace = WorkspaceClient()
    bucket, key = location.netloc, location.path.lstrip("/")
    objects = [key] if not key.endswith("/") else [o["Key"] for o in client.list_objects_v2(Bucket=bucket, Prefix=key).get("Contents", []) if not o["Key"].endswith("/")]
    if not objects:
        raise FileNotFoundError(f"No S3 objects found at {source.options['path']}")
    _reset_landing_folder(dbutils, target)
    workspace.files.create_directory(target)
    for object_key in objects:
        body = client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
        filename = object_key.rsplit("/", 1)[-1]
        workspace.files.upload(f"{target}/{filename}", io.BytesIO(body), overwrite=True)
        log(get_logger(), "s3 object landed", source=source.name, key=object_key, bytes=len(body), target=target)
    return target


def _reset_landing_folder(dbutils, target: str) -> None:
    try:
        dbutils.fs.rm(target, recurse=True)
    except Exception:  # noqa: BLE001 - folder may not exist yet
        pass


def extract_and_land(spark, dbutils, source: Source, secret_scope: str, volume_path: str, run_date: str) -> tuple[str, int | None]:
    """Extract from the source system and land in the raw volume. Returns (path, rows) - rows is None for byte copies."""
    target = landing_path(volume_path, source, run_date)
    if source.type == "s3":
        return land_s3_object(dbutils, source, secret_scope, target), None
    if source.type == "sqlserver":
        df = read_sqlserver(spark, dbutils, source, secret_scope)
    elif source.type == "cosmos_mongodb":
        df = read_cosmos(spark, dbutils, source, secret_scope)
    else:
        raise ValueError(f"Unsupported source type: {source.type}")
    land_dataframe(df, source, target)
    return target, df.count()


# --------------------------------------------------------------------------- #
# Reading landed data back (Bronze)
# --------------------------------------------------------------------------- #
def landing_exists(dbutils, path: str) -> bool:
    try:
        return len(dbutils.fs.ls(path)) > 0
    except Exception:  # noqa: BLE001 - dbutils raises a generic exception for missing paths
        return False


def read_landed_raw(spark, dbutils, source: Source, volume_path: str, run_date: str):
    """Read the source-specific original-format files landed by an ingestion task."""
    path = landing_path(volume_path, source, run_date)
    if not landing_exists(dbutils, path):
        raise FileNotFoundError(
            f"Raw landing path is missing: {path}. Run the ingestion task for source '{source.name}' with run_date={run_date} before Bronze."
        )
    reader = spark.read.format(source.format)
    if source.format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true").option("escape", '"').option("multiLine", "true")
    if source.format == "json":
        reader = reader.option("multiLine", "false")
    df = reader.load(path).withColumn("_source_file", F.col("_metadata.file_path"))
    for column, spark_type in source.schema_hints.items():
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast(spark_type))
    return df, path
