import json


def _secret(dbutils, scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def read_source(spark, dbutils, source: dict, secret_scope: str):
    source_type = source["type"]
    if source_type == "s3":
        return spark.read.format(source["format"]).load(source["path"])
    if source_type == "sqlserver":
        keys = source["secret_keys"]
        url = f"jdbc:sqlserver://{source['host']}:{source.get('port', 1433)};databaseName={source['database']};encrypt=true;trustServerCertificate=false"
        return (spark.read.format("jdbc").option("url", url).option("dbtable", source["table"])
                .option("user", _secret(dbutils, secret_scope, keys["username"]))
                .option("password", _secret(dbutils, secret_scope, keys["password"]))
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())
    if source_type == "cosmos_mongodb":
        # The Python client works on Databricks Serverless; it avoids a cluster-scoped
        # JVM Mongo Spark connector dependency for this small batch source.
        from bson import json_util
        from pymongo import MongoClient

        connection = _secret(dbutils, secret_scope, source["secret_keys"]["connection_string"])
        client = MongoClient(connection, serverSelectionTimeoutMS=30_000)
        documents = list(client[source["database"]][source["collection"]].find({}))
        if not documents:
            raise ValueError(f"Cosmos collection is empty: {source['database']}.{source['collection']}")
        # SparkContext is unavailable on Serverless compute. Serialise all nested
        # BSON values to JSON strings so inconsistent array/document shapes (for
        # example ordered_products) do not break Spark schema inference.
        def serialise_value(value):
            if isinstance(value, (dict, list)):
                return json_util.dumps(value)
            if value is None or isinstance(value, (str, int, float, bool)):
                return value
            return str(value)

        json_rows = [
            {field: serialise_value(value) for field, value in document.items()}
            for document in documents
        ]
        return spark.createDataFrame(json_rows)
    raise ValueError(f"Unsupported source type: {source_type}")


def land_raw(spark, dbutils, df, source: dict, volume_path: str, load_date: str) -> str:
    """Land original file types where possible; relational/document sources are serialized once."""
    target = f"{volume_path}/{source['name']}/load_date={load_date}"
    if source["type"] == "s3":
        dbutils.fs.mkdirs(target)
        dbutils.fs.cp(source["path"], target, recurse=True)
        return target
    if source["type"] == "cosmos_mongodb":
        df.write.format("json").mode("overwrite").save(target)
    else:
        df.write.mode("overwrite").option("header", "true").csv(target)
    return target


def read_landed_raw(spark, source: dict, volume_path: str, load_date: str):
    """Read the source-specific original-format files landed by an ingestion task."""
    path = f"{volume_path}/{source['name']}/load_date={load_date}"
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    file_system = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not file_system.exists(hadoop_path):
        raise FileNotFoundError(
            f"Raw landing path is missing: {path}. Run the ingestion task for "
            f"source '{source['name']}' with run_date={load_date} before DS2B."
        )
    reader = spark.read.format(source["format"])
    if source["format"] == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    return reader.load(path), path
