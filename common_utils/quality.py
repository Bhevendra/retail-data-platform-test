from datetime import datetime, timezone
from pyspark.sql import functions as F


def evaluate_rules(df, rules: list[dict]) -> list[dict]:
    """Evaluate configured blocking/warning data checks without collecting data."""
    results = []
    for rule in rules:
        column = rule["column"]
        if rule["type"] == "not_null":
            failed = df.filter(F.col(column).isNull()).count()
        elif rule["type"] == "unique":
            failed = df.groupBy(column).count().filter(F.col("count") > 1).agg(F.sum("count").alias("n")).first()["n"] or 0
        elif rule["type"] == "accepted_values":
            failed = df.filter(~F.col(column).isin(rule["values"]) | F.col(column).isNull()).count()
        else:
            raise ValueError(f"Unsupported quality rule type: {rule['type']}")
        results.append({**rule, "failed_rows": int(failed), "passed": failed == 0})
    return results


def persist_results(spark, catalog: str, schema: str, table: str, entity: str, run_id: str, results: list[dict]) -> None:
    rows = [(run_id, entity, r["name"], r["severity"], r["failed_rows"], r["passed"], datetime.now(timezone.utc)) for r in results]
    if not rows:
        return
    output = spark.createDataFrame(rows, "run_id string, entity string, rule_name string, severity string, failed_rows long, passed boolean, evaluated_at timestamp")
    output.write.mode("append").saveAsTable(f"{catalog}.{schema}.{table}")


def raise_for_blocking_failures(results: list[dict]) -> None:
    failed = [r["name"] for r in results if not r["passed"] and r["severity"].lower() == "error"]
    if failed:
        raise ValueError(f"Blocking data-quality rules failed: {', '.join(failed)}")

