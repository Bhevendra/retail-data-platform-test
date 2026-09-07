"""Declarative data-quality rule engine.

Rules are data, not code: a list of dictionaries describing what "good" means.
The engine evaluates them in a single Spark pass where possible and returns
counts — it never changes or blocks the data. Deciding what to do about a
failure is the caller's job (here: report it and alert).

Rule types
----------
``not_null``  ``unique``  ``accepted_values``  ``regex``  ``range``
``min_row_count``  ``expression`` (a SQL boolean that must hold for every row)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RESULTS_TABLE = "data_quality_results"
RESULTS_SCHEMA = (
    "run_date date, checked_at timestamp, layer string, table_name string, rule_name string, rule_type string, "
    "column_name string, severity string, rows_checked long, failed_rows long, passed boolean, description string"
)
ROW_LEVEL_TYPES = {"not_null", "accepted_values", "regex", "range", "expression"}
DATASET_TYPES = {"unique", "min_row_count"}


@dataclass(frozen=True)
class RuleResult:
    rule: dict
    rows_checked: int
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    @property
    def is_error(self) -> bool:
        return not self.passed and self.rule.get("severity", "error") == "error"


def violation_predicate(rule: dict):
    """A Column that is TRUE for rows that BREAK the rule (None for dataset-level rules)."""
    rule_type = rule["type"]
    if rule_type in DATASET_TYPES:
        return None
    if rule_type == "expression":
        expression = F.expr(rule["expression"])
        return ~expression | expression.isNull()
    column = F.col(rule["column"])
    if rule_type == "not_null":
        return column.isNull()
    if rule_type == "accepted_values":
        return column.isNull() | ~column.isin(rule["values"])
    if rule_type == "regex":
        return column.isNull() | ~column.cast("string").rlike(rule["pattern"])
    if rule_type == "range":
        violation = column.isNull()
        if rule.get("min") is not None:
            violation = violation | (column < F.lit(rule["min"]))
        if rule.get("max") is not None:
            violation = violation | (column > F.lit(rule["max"]))
        return violation
    raise ValueError(f"Unsupported rule type: {rule_type}")


def evaluate_rules(df: DataFrame, rules: list[dict]) -> list[RuleResult]:
    """Evaluate every rule. Row-level rules are counted in one aggregation."""
    if not rules:
        return []
    aggregations = [F.count(F.lit(1)).alias("__rows")]
    for index, rule in enumerate(rules):
        predicate = violation_predicate(rule)
        if predicate is not None:
            aggregations.append(F.sum(F.when(predicate, 1).otherwise(0)).alias(f"__r{index}"))
    summary = df.agg(*aggregations).first()
    rows_checked = int(summary["__rows"])

    results = []
    for index, rule in enumerate(rules):
        if rule["type"] == "unique":
            duplicates = df.groupBy(rule["column"]).count().filter("count > 1").agg(F.coalesce(F.sum("count"), F.lit(0)).alias("n")).first()["n"]
            failed = int(duplicates)
        elif rule["type"] == "min_row_count":
            failed = 0 if rows_checked >= rule["min"] else 1
        else:
            failed = int(summary[f"__r{index}"] or 0)
        results.append(RuleResult(rule, rows_checked, failed))
    return results


def failing_rows(df: DataFrame, rules: list[dict], limit: int = 20) -> DataFrame:
    """Sample of rows breaking any row-level rule, with the rule names — for investigation."""
    checks = [(r["name"], violation_predicate(r)) for r in rules if violation_predicate(r) is not None]
    if not checks:
        return df.limit(0)
    failed = F.array_compact(F.array(*[F.when(predicate, F.lit(name)) for name, predicate in checks]))
    return df.withColumn("_failed_rules", failed).filter(F.size("_failed_rules") > 0).limit(limit)


def ensure_results_table(spark, catalog: str, schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{RESULTS_TABLE}` ({RESULTS_SCHEMA})
        USING DELTA
        COMMENT 'One row per data-quality rule per run. Checks report; they never block or change data.'
        """
    )


def persist_results(spark, catalog: str, schema: str, run_date: str, layer: str, table_name: str, results: list[RuleResult]) -> None:
    if not results:
        return
    checked_at = datetime.now(timezone.utc)
    rows = [
        (
            datetime.strptime(run_date, "%Y-%m-%d").date(),
            checked_at,
            layer,
            table_name,
            r.rule["name"],
            r.rule["type"],
            r.rule.get("column"),
            r.rule.get("severity", "error"),
            r.rows_checked,
            r.failed_rows,
            r.passed,
            r.rule.get("description"),
        )
        for r in results
    ]
    spark.createDataFrame(rows, RESULTS_SCHEMA).write.mode("append").saveAsTable(f"`{catalog}`.`{schema}`.`{RESULTS_TABLE}`")
