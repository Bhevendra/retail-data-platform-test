# Class 18 — Data quality engine

## Objectives

* Write quality checks as data (rules in config) rather than ad-hoc `if` statements.
* Implement row-level rules (`not_null`, `accepted_values`, `regex`, `range`, `expression`) and dataset-level rules (`unique`, `min_row_count`) in one Spark pass.
* Choose severity (`error`/`warn`) and behaviour (`fail`/`quarantine`/`warn`) per entity.
* Persist results to `ops.data_quality_results` and query trends.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–15 | Why "it looked fine" is not a quality strategy; the three questions |
| 15–40 | v1: hand-written checks → v2: rule dictionaries → predicate function |
| 40–60 | `evaluate_rules` in a single aggregation |
| 60–75 | Quarantine: `split_quarantine` and the `_quarantine` table |
| 75–90 | Results table, wiring into Bronze/Silver/Gold, severity decisions for our data |
| 90–100 | Homework |

## The three questions (15 min)

For every check: *what* (rule), *how bad* (severity), *what then* (behaviour). Show the
matrix for our data from Class 8: duplicate `order_number` in Bronze is `unique/warn`
(expected, Silver handles it); missing `customer_id` is `not_null/error` with
`quarantine` (keep the good rows moving); an empty extract is `min_row_count/error`
(nothing to quarantine — stop).

## From checks to rules (25 min)

v1 — what students would write today:

```python
nulls = df.filter(F.col("customer_id").isNull()).count()
if nulls > 0:
    raise ValueError(f"customer_id has {nulls} nulls")
```

v2 — the rule as data plus a function that turns a rule into a *violation predicate*:

```python
rule = {"name": "customer_id_not_null", "type": "not_null", "column": "customer_id", "severity": "error"}

def row_predicate(rule):
    column = F.col(rule["column"]) if rule.get("column") else None
    if rule["type"] == "not_null":
        return column.isNull()
    if rule["type"] == "accepted_values":
        return column.isNull() | ~column.isin(rule["values"])
    if rule["type"] == "regex":
        return column.isNull() | ~column.cast("string").rlike(rule["pattern"])
    if rule["type"] == "range":
        v = column.isNull()
        if rule.get("min") is not None: v = v | (column < F.lit(rule["min"]))
        if rule.get("max") is not None: v = v | (column > F.lit(rule["max"]))
        return v
    if rule["type"] == "expression":
        return ~F.expr(rule["expression"]) | F.expr(rule["expression"]).isNull()
    return None    # unique, min_row_count are dataset-level
```

"A predicate is a column that is TRUE for rows that break the rule." Test each type on
`customers` with `df.filter(row_predicate(rule)).count()`.

## One pass (20 min)

Counting each rule separately scans the table once per rule. Instead, one `agg` with a
`sum(when(predicate, 1))` per rule:

```python
def evaluate_rules(df, rules):
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for i, rule in enumerate(rules):
        p = row_predicate(rule)
        if p is not None:
            aggs.append(F.sum(F.when(p, 1).otherwise(0)).alias(f"__r{i}"))
    summary = df.agg(*aggs).first()
    rows = int(summary["__rows"])
    results = []
    for i, rule in enumerate(rules):
        if rule["type"] == "unique":
            failed = df.groupBy(rule["column"]).count().filter("count > 1").agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
        elif rule["type"] == "min_row_count":
            failed = 0 if rows >= rule["min"] else 1
        else:
            failed = int(summary[f"__r{i}"] or 0)
        results.append({"rule": rule, "rows_checked": rows, "failed_rows": int(failed), "passed": failed == 0})
    return results
```

`enumerate` is new (index + item). Run the seven-rule example from `tests/test_transforms.py`
and compare counts by hand on the four-row toy DataFrame — that test *is* the lesson.

## Quarantine (15 min)

```python
def split_quarantine(df, rules):
    checks = [(r["name"], row_predicate(r)) for r in rules if r["severity"] == "error" and row_predicate(r) is not None]
    failed = F.array_compact(F.array(*[F.when(p, F.lit(name)) for name, p in checks]))
    flagged = df.withColumn("_failed_rules", failed)
    return flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules"), flagged.filter(F.size("_failed_rules") > 0)
```

Each row gets the list of rules it broke; good rows go to Bronze, bad rows to
`bronze.<entity>_quarantine` with `_failed_rules` — still idempotent per `_load_date` via
`write_idempotent`. Dataset-level error rules cannot be quarantined row by row and still
block. Show `load_source_to_bronze` in `common_utils/bronze.py`: the three branches
`fail / quarantine / warn`.

## Results table and wiring (15 min)

`ops.data_quality_results`: one row per rule per run with `rows_checked`, `failed_rows`,
`passed`. Bronze, Silver and Gold all call `evaluate_rules → persist_results →
raise_for_blocking_failures`. Decide severities for our findings together and put them in
the three JSON files; run the pipeline; query:

```sql
SELECT run_date, layer, entity, rule_name, severity, failed_rows FROM retaildataplatform.ops.data_quality_results
WHERE NOT passed ORDER BY run_date DESC;
```

Expected warnings: `order_ts_present` (45), `line_count_consistent` (147), Bronze
`unique_order_number`.

## Homework

1. Add a `regex` rule for `state` (`^[A-Z]{2}$`) as `warn` and a `range` rule for `lat` and `lon`. Run and read the results.
2. Switch `sqlserver_customers` to `on_quality_failure: "fail"` and break a rule; then switch back to `quarantine` and inspect `customers_quarantine`.
3. Propose one rule that would have caught the broken product JSON in `sales` at Silver level (`expression`).

## Common problems

* `expression` rules must be TRUE for *valid* rows; students write the violation. Read the docstring in `quality.py`.
* `unique` on a column with NULLs: NULLs group together — decide whether that matters (usually a separate `not_null` rule).
* Quarantine table schema differs from the main table by `_failed_rules` — never union them without dropping it.
