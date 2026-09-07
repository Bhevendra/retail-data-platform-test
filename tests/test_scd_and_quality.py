"""Unit tests for the SCD helpers and the data-quality rule engine.

The merges themselves need Delta, so they live in the integration tests; what is
tested here is the logic that decides *what* to merge and *what* counts as a failure.
"""

import pytest
from pyspark.sql import Row

from common_utils.metadata import AUDIT_COLUMNS, add_metadata_columns
from common_utils.quality import RuleResult, evaluate_rules, failing_rows, violation_predicate
from common_utils.scd import HASH_COLUMN, business_columns, deduplicate, row_hash


# --------------------------------------------------------------------------- #
# metadata
# --------------------------------------------------------------------------- #
def test_add_metadata_columns_adds_exactly_three(spark):
    df = spark.createDataFrame([(1,)], "id int")
    out = add_metadata_columns(df, load_date="2026-09-07", source_file="/Volumes/x/y.csv")
    assert [c for c in out.columns if c.startswith("_")] == AUDIT_COLUMNS
    row = out.first()
    assert str(row["_load_date"]) == "2026-09-07" and row["_source_file"] == "/Volumes/x/y.csv"


# --------------------------------------------------------------------------- #
# hashing and de-duplication
# --------------------------------------------------------------------------- #
def test_row_hash_is_null_safe_and_order_independent(spark):
    df = spark.createDataFrame([("x", None, 1)], "a string, b string, c int")
    first = row_hash(df, ["a", "b", "c"]).first()[HASH_COLUMN]
    reordered = row_hash(df, ["c", "a", "b"]).first()[HASH_COLUMN]
    empty_string = row_hash(spark.createDataFrame([("x", "", 1)], "a string, b string, c int"), ["a", "b", "c"]).first()[HASH_COLUMN]

    assert first == reordered, "column order must not change the hash"
    assert first != empty_string, "NULL and empty string must hash differently"
    assert len(first) == 64


def test_business_columns_excludes_technical_ones(spark):
    df = spark.createDataFrame([(1, "a", "h")], "id int, name string, _row_hash string")
    df = add_metadata_columns(df, "2026-09-07", "f")
    assert business_columns(df) == ["id", "name"]


def test_deduplicate_keeps_the_latest_version(spark):
    df = spark.createDataFrame([Row(id=1, v="old", ts=1), Row(id=1, v="new", ts=2), Row(id=2, v="only", ts=1)])
    rows = {r["id"]: r["v"] for r in deduplicate(df, ["id"], order_by="ts").collect()}
    assert rows == {1: "new", 2: "only"}


def test_deduplicate_on_a_composite_key(spark):
    df = spark.createDataFrame(
        [Row(order=1, line=1, v="old", doc="a"), Row(order=1, line=1, v="new", doc="b"), Row(order=1, line=2, v="x", doc="b")]
    )
    result = deduplicate(df, ["order", "line"], order_by="doc")
    assert result.count() == 2
    assert result.filter("line = 1").first()["v"] == "new"


# --------------------------------------------------------------------------- #
# quality rules
# --------------------------------------------------------------------------- #
@pytest.fixture
def customers(spark):
    return spark.createDataFrame(
        [
            Row(id=1, email="a@x.com", status="active", age=30),
            Row(id=2, email=None, status="active", age=200),
            Row(id=2, email="c@x.com", status="weird", age=40),
            Row(id=4, email="bad", status="inactive", age=None),
        ]
    )


def test_every_rule_type_counts_correctly(customers):
    rules = [
        {"name": "id_not_null", "type": "not_null", "column": "id"},
        {"name": "unique_id", "type": "unique", "column": "id"},
        {"name": "status_values", "type": "accepted_values", "column": "status", "values": ["active", "inactive"]},
        {"name": "email_format", "type": "regex", "column": "email", "pattern": r"^[^@]+@[^@]+\.[^@]+$"},
        {"name": "age_range", "type": "range", "column": "age", "min": 0, "max": 120},
        {"name": "min_rows", "type": "min_row_count", "min": 10},
        {"name": "expr", "type": "expression", "expression": "age IS NULL OR age < 100"},
    ]
    results = {r.rule["name"]: r for r in evaluate_rules(customers, rules)}

    assert results["id_not_null"].failed_rows == 0 and results["id_not_null"].passed
    assert results["unique_id"].failed_rows == 2, "both rows sharing id=2 are counted"
    assert results["status_values"].failed_rows == 1
    assert results["email_format"].failed_rows == 2, "NULL and 'bad'"
    assert results["age_range"].failed_rows == 2, "200 and NULL"
    assert results["min_rows"].failed_rows == 1 and results["min_rows"].rows_checked == 4
    assert results["expr"].failed_rows == 1
    assert all(r.rows_checked == 4 for r in results.values())


def test_dataset_level_rules_have_no_row_predicate():
    assert violation_predicate({"type": "unique", "column": "id"}) is None
    assert violation_predicate({"type": "min_row_count", "min": 1}) is None
    assert violation_predicate({"type": "not_null", "column": "id"}) is not None


def test_failing_rows_names_the_rules_each_row_broke(customers):
    rules = [
        {"name": "email_not_null", "type": "not_null", "column": "email"},
        {"name": "age_range", "type": "range", "column": "age", "min": 0, "max": 120},
    ]
    broken = {r["id"]: set(r["_failed_rules"]) for r in failing_rows(customers, rules).collect()}
    assert broken == {2: {"email_not_null", "age_range"}, 4: {"age_range"}}


def test_rule_result_severity_logic():
    error = RuleResult({"name": "r", "type": "not_null", "severity": "error"}, 10, 1)
    warn = RuleResult({"name": "r", "type": "not_null", "severity": "warn"}, 10, 1)
    clean = RuleResult({"name": "r", "type": "not_null", "severity": "error"}, 10, 0)

    assert error.is_error and not error.passed
    assert not warn.is_error, "a warning is never an error, however many rows failed"
    assert clean.passed and not clean.is_error
