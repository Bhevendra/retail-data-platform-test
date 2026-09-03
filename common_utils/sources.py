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
        connection = _secret(dbutils, secret_scope, source["secret_keys"]["connection_string"])
        return (spark.read.format("mongodb").option("connection.uri", connection)
                .option("database", source["database"]).option("collection", source["collection"]).load())
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
    reader = spark.read.format(source["format"])
    if source["format"] == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")
    return reader.load(path), path
