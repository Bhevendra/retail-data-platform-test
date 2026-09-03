"""Pure DataFrame logic that runs on local Spark: quality rules, hashing, dedupe, transformations, date dimension."""

from datetime import date

import pytest
from pyspark.sql import Row

from common_utils.config import QualityRule, SilverEntity, Transformations
from common_utils.gold import date_dimension, render_sql
from common_utils.quality import evaluate_rules, split_quarantine
from common_utils.runtime import RunContext, parse_run_date
from common_utils.scd import HASH_COLUMN, deduplicate, row_hash
from common_utils.silver import apply_transformations, business_columns, prepare


def rule(**kwargs):
    return QualityRule.from_dict({"severity": "error", **kwargs}, "test")


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


def test_evaluate_rules_counts_failures_per_rule(customers):
    rules = [
        rule(name="id_not_null", type="not_null", column="id"),
        rule(name="unique_id", type="unique", column="id"),
        rule(name="status_values", type="accepted_values", column="status", values=["active", "inactive"]),
        rule(name="email_format", type="regex", column="email", pattern=r"^[^@]+@[^@]+\.[^@]+$"),
        rule(name="age_range", type="range", column="age", min=0, max=120),
        rule(name="min_rows", type="min_row_count", min=10),
        rule(name="expr", type="expression", expression="age IS NULL OR age < 100"),
    ]
    results = {r.rule.name: r for r in evaluate_rules(customers, rules)}
    assert results["id_not_null"].failed_rows == 0 and results["id_not_null"].passed
    assert results["unique_id"].failed_rows == 2  # both rows sharing id=2
    assert results["status_values"].failed_rows == 1
    assert results["email_format"].failed_rows == 2  # null + "bad"
    assert results["age_range"].failed_rows == 2  # 200 and null
    assert results["min_rows"].failed_rows == 1 and results["min_rows"].rows_checked == 4
    assert results["expr"].failed_rows == 1
    assert all(r.rows_checked == 4 for r in results.values())


def test_split_quarantine_separates_rows_and_records_failed_rules(customers):
    rules = [
        rule(name="email_not_null", type="not_null", column="email"),
        rule(name="age_range", type="range", column="age", min=0, max=120),
        rule(name="warn_only", type="accepted_values", column="status", values=["active"], severity="warn"),
        rule(name="unique_id", type="unique", column="id"),  # dataset-level: ignored by quarantine
    ]
    valid, quarantined = split_quarantine(customers, rules)
    assert valid.count() == 2 and "_failed_rules" not in valid.columns
    bad = {r["id"]: set(r["_failed_rules"]) for r in quarantined.collect()}
    assert bad == {2: {"email_not_null", "age_range"}, 4: {"age_range"}}


def test_row_hash_is_null_safe_order_independent_and_stable(spark):
    df = spark.createDataFrame([("x", None, 1)], "a string, b string, c int")
    h1 = row_hash(df, ["a", "b", "c"]).first()[HASH_COLUMN]
    h2 = row_hash(df, ["c", "a", "b"]).first()[HASH_COLUMN]
    h3 = row_hash(spark.createDataFrame([("x", "", 1)], "a string, b string, c int"), ["a", "b", "c"]).first()[HASH_COLUMN]
    assert h1 == h2 and len(h1) == 64
    assert h1 != h3, "null and empty string must hash differently"


def test_deduplicate_keeps_latest_by_order_column(spark):
    df = spark.createDataFrame([Row(id=1, v="old", ts=1), Row(id=1, v="new", ts=2), Row(id=2, v="only", ts=1)])
    rows = {r["id"]: r["v"] for r in deduplicate(df, ["id"], "ts").collect()}
    assert rows == {1: "new", 2: "only"}


def test_apply_transformations_in_documented_order(spark):
    df = spark.createDataFrame([Row(CustName="  Ada ", amt="12.50", meta='{"k": 1}', junk="NULL", keep=1)])
    t = Transformations.from_dict(
        {
            "trim": ["CustName"],
            "rename": {"CustName": "customer_name", "amt": "amount"},
            "cast": {"amount": "decimal(10,2)"},
            "parse_json": {"meta": "struct<k:int>"},
            "derived": {"amount_with_vat": "amount * 1.2", "name_upper": "upper(customer_name)"},
            "drop": ["meta"],
        }
    )
    row = apply_transformations(df, t).first()
    assert row["customer_name"] == "Ada" and row["name_upper"] == "ADA"
    assert str(row["amount"]) == "12.50" and float(row["amount_with_vat"]) == 15.0
    assert row["junk"] is None, "NULL literal should be normalised to null"
    assert "meta" not in row.asDict()


def test_prepare_hashes_business_columns_only_and_adds_lineage(spark):
    entity = SilverEntity.from_dict({"source_table": "c", "target_table": "c", "primary_keys": ["id"], "scd_type": 2, "order_by": "_ingested_at"})
    ctx = RunContext(environment="test", catalog="cat", secret_scope="s", run_date=date(2026, 9, 3), run_id="r1")
    df = spark.createDataFrame(
        [(1, "a", 1, "old", None, "x", "f", None, "p")],
        "id int, name string, _ingested_at int, _run_id string, _load_date date, _source_system string, _source_file string, last_update_ts timestamp, file_path string",
    )
    out = prepare(df, entity, ctx)
    assert business_columns(out, entity) == ["id", "name"]
    row = out.first()
    assert row["_run_id"] == "r1" and str(row["_load_date"]) == "2026-09-03"
    assert "_source_system" not in out.columns and HASH_COLUMN in out.columns


def test_prepare_fails_when_key_missing(spark):
    entity = SilverEntity.from_dict({"source_table": "c", "target_table": "c", "primary_keys": ["missing"], "scd_type": 1})
    ctx = RunContext(environment="t", catalog="c", secret_scope="s", run_date=date.today())
    with pytest.raises(ValueError, match="primary key columns missing"):
        prepare(spark.createDataFrame([Row(id=1)]), entity, ctx)


def test_date_dimension_covers_range_with_expected_attributes(spark):
    df = date_dimension(spark, "2026-01-01", "2026-01-31")
    assert df.count() == 31
    first = df.orderBy("date_key").first()
    assert first["date_key"] == 20260101 and first["year_quarter"] == "2026-Q1" and first["day_name"] == "Thursday"
    assert df.filter("is_weekend").count() == 9  # Jan 2026 starts on a Thursday


def test_render_sql_replaces_placeholders():
    assert render_sql("select * from ${catalog}.${silver}.a join ${catalog}.${gold}.b", "c", "s", "g") == "select * from c.s.a join c.g.b"


@pytest.mark.parametrize("value, expected", [("2026-09-03", date(2026, 9, 3)), ("2026-09-03T05:00:00Z", date(2026, 9, 3)), ("", date.today()), ("{{job.start_time.iso_date}}", date.today()), (None, date.today())])
def test_parse_run_date_accepts_job_parameter_forms(value, expected):
    assert parse_run_date(value) == expected


def test_cosmos_schema_inference_handles_nulls_and_mixed_types(spark):
    from common_utils.sources import _coerce, infer_schema

    rows = [{"a": 1, "b": None, "c": "x", "d": 1.5, "e": True}, {"a": 2, "b": None, "c": 3, "d": 2}]
    schema = infer_schema(rows)
    assert schema == [("a", "long"), ("b", "string"), ("c", "string"), ("d", "double"), ("e", "boolean")]
    data = [tuple(_coerce(r.get(n), t) for n, t in schema) for r in rows]
    df = spark.createDataFrame(data, ", ".join(f"`{n}` {t}" for n, t in schema))
    assert df.count() == 2 and df.filter("c = '3'").count() == 1


def test_tolerant_cast_accepts_float_formatted_integers_and_nulls_bad_values(spark):
    df = spark.createDataFrame([("1.564627663E9", "46506.0", "12", "abc", None)], "a string, b string, c string, d string, e string")
    t = Transformations.from_dict({"cast": {"a": "bigint", "b": "int", "c": "bigint", "d": "int", "e": "bigint"}, "null_literals": []})
    row = apply_transformations(df, t).first()
    assert (row["a"], row["b"], row["c"], row["d"], row["e"]) == (1564627663, 46506, 12, None, None)
