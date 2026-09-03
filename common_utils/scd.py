from functools import reduce
from pyspark.sql import functions as F


def row_hash(df, columns: list[str]):
    return df.withColumn("_row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in columns]), 256))


def merge_type_1(spark, source_df, target: str, keys: list[str]) -> None:
    source_df.createOrReplaceTempView("_scd_source")
    clauses = " AND ".join([f"t.`{key}` = s.`{key}`" for key in keys])
    spark.sql(f"MERGE INTO {target} t USING _scd_source s ON {clauses} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")


def merge_type_2(spark, source_df, target: str, keys: list[str]) -> None:
    if not spark.catalog.tableExists(target.replace('`', '')):
        (source_df.withColumn("effective_from", F.current_timestamp()).withColumn("effective_to", F.lit(None).cast("timestamp")).withColumn("is_current", F.lit(True))
         .write.format("delta").saveAsTable(target))
        return
    current = spark.table(target).filter("is_current = true").select(*keys, F.col("_row_hash").alias("_existing_hash"))
    changes = source_df.alias("s").join(current.alias("t"), keys, "left").filter(F.col("_existing_hash").isNull() | (F.col("s._row_hash") != F.col("_existing_hash"))).drop("_existing_hash")
    if changes.limit(1).count() == 0:
        return
    changes.createOrReplaceTempView("_scd_changes")
    join_condition = " AND ".join([f"t.`{key}` = s.`{key}`" for key in keys])
    spark.sql(f"MERGE INTO {target} t USING _scd_changes s ON {join_condition} AND t.is_current = true WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET is_current = false, effective_to = current_timestamp()")
    (changes.withColumn("effective_from", F.current_timestamp()).withColumn("effective_to", F.lit(None).cast("timestamp")).withColumn("is_current", F.lit(True))
     .write.mode("append").format("delta").saveAsTable(target))
