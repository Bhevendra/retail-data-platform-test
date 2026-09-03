"""Declarative data-quality engine.

Rules are declared per entity in configuration and evaluated in a *single*
Spark pass where possible. Every evaluation is persisted to
``<catalog>.ops.data_quality_results`` so quality trends are queryable, and
failing rows can be quarantined instead of blocking the whole load.

Supported rule types
--------------------
not_null, unique, accepted_values, regex, range, min_row_count, expression
(an arbitrary SQL boolean expression that must hold for every row).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from retail_platform.config import QualityRule, qualified
from retail_platform.runtime import RunContext

RESULTS_TABLE = "data_quality_results"
RESULTS_SCHEMA = (
    "run_id string, run_date date, layer string, entity string, rule_name string, rule_type string, column_name string, "
    "severity string, rows_checked long, failed_rows long, passed boolean, evaluated_at timestamp"
)


@dataclass(frozen=True)
class RuleResult:
    rule: QualityRule
    rows_checked: int
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    @property
    def blocking(self) -> bool:
        return not self.passed and self.rule.severity == "error"


def row_predicate(rule: QualityRule):
    """Return a Column that is TRUE for rows that VIOLATE the rule (None for dataset-level rules)."""
    column = F.col(rule.column) if rule.column else None
    if rule.type == "not_null":
        return column.isNull()
    if rule.type == "accepted_values":
        return column.isNull() | ~column.isin(rule.values)
    if rule.type == "regex":
        return column.isNull() | ~column.cast("string").rlike(rule.pattern)
    if rule.type == "range":
        violation = column.isNull()
        if rule.min is not None:
            violation = violation | (column < F.lit(rule.min))
        if rule.max is not None:
            violation = violation | (column > F.lit(rule.max))
        return violation
    if rule.type == "expression":
        return ~F.expr(rule.expression) | F.expr(rule.expression).isNull()
    return None  # unique, min_row_count are dataset-level


def evaluate_rules(df: DataFrame, rules: list[QualityRule]) -> list[RuleResult]:
    """Evaluate all rules. Row-level rules are aggregated in one pass; dataset-level rules separately."""
    if not rules:
        return []
    aggregations = [F.count(F.lit(1)).alias("__rows")]
    for index, rule in enumerate(rules):
        predicate = row_predicate(rule)
        if predicate is not None:
            aggregations.append(F.sum(F.when(predicate, 1).otherwise(0)).alias(f"__r{index}"))
    summary = df.agg(*aggregations).first()
    rows_checked = int(summary["__rows"])
    results: list[RuleResult] = []
    for index, rule in enumerate(rules):
        if rule.type == "unique":
            duplicate_rows = (
                df.groupBy(rule.column).count().filter(F.col("count") > 1).agg(F.coalesce(F.sum("count"), F.lit(0)).alias("n")).first()["n"]
            )
            results.append(RuleResult(rule, rows_checked, int(duplicate_rows)))
        elif rule.type == "min_row_count":
            results.append(RuleResult(rule, rows_checked, 0 if rows_checked >= rule.min else 1))
        else:
            results.append(RuleResult(rule, rows_checked, int(summary[f"__r{index}"] or 0)))
    return results


def split_quarantine(df: DataFrame, rules: list[QualityRule]) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, quarantined) using the row-level *error* rules. Quarantined rows carry the failed rule names."""
    checks = [(rule.name, row_predicate(rule)) for rule in rules if rule.severity == "error" and row_predicate(rule) is not None]
    if not checks:
        return df, df.limit(0).withColumn("_failed_rules", F.array().cast("array<string>"))
    failed = F.array_compact(F.array(*[F.when(predicate, F.lit(name)) for name, predicate in checks]))
    flagged = df.withColumn("_failed_rules", failed)
    valid = flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    quarantined = flagged.filter(F.size("_failed_rules") > 0)
    return valid, quarantined


def persist_results(spark, ctx: RunContext, catalog: str, ops_schema: str, layer: str, entity: str, results: list[RuleResult]) -> None:
    if not results:
        return
    now = datetime.now(timezone.utc)
    rows = [
        (ctx.run_id, ctx.run_date, layer, entity, r.rule.name, r.rule.type, r.rule.column, r.rule.severity, r.rows_checked, r.failed_rows, r.passed, now)
        for r in results
    ]
    spark.createDataFrame(rows, RESULTS_SCHEMA).write.mode("append").saveAsTable(qualified(catalog, ops_schema, RESULTS_TABLE))


def ensure_results_table(spark, catalog: str, ops_schema: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified(catalog, ops_schema, RESULTS_TABLE)} ({RESULTS_SCHEMA})
        USING DELTA
        COMMENT 'Result of every configured data-quality rule per run. failed_rows > 0 with severity error blocks or quarantines.'
        """
    )


def blocking_failures(results: list[RuleResult]) -> list[RuleResult]:
    return [r for r in results if r.blocking]


def raise_for_blocking_failures(results: list[RuleResult], entity: str) -> None:
    failed = blocking_failures(results)
    if failed:
        detail = ", ".join(f"{r.rule.name} ({r.failed_rows} rows)" for r in failed)
        raise ValueError(f"Blocking data-quality rules failed for {entity}: {detail}")
