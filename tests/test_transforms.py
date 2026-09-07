"""Unit tests for the generic transformation helpers."""

import pytest

from common_utils.transforms import (
    add_derived,
    cast_columns,
    drop_columns,
    explode_array,
    normalise_nulls,
    parse_json_column,
    rename_columns,
    strip_float_suffix,
    trim_columns,
)


def test_normalise_nulls_turns_text_nulls_into_real_nulls(spark):
    df = spark.createDataFrame([("NULL", "NA NA", "  ", "real")], "a string, b string, c string, d string")
    row = normalise_nulls(df).first()
    assert (row["a"], row["b"], row["c"], row["d"]) == (None, None, None, "real")


def test_trim_collapses_inner_whitespace(spark):
    df = spark.createDataFrame([("  SMITH,   SHIRLEY  ",)], "name string")
    assert trim_columns(df, ["name"]).first()["name"] == "SMITH, SHIRLEY"


def test_rename_ignores_missing_columns(spark):
    df = spark.createDataFrame([(1,)], "a int")
    renamed = rename_columns(df, {"a": "b", "does_not_exist": "c"})
    assert renamed.columns == ["b"]


@pytest.mark.parametrize(
    "value, dtype, expected",
    [
        ("1.564627663E9", "bigint", 1564627663),  # Cosmos writes epochs as floats
        ("46506.0", "int", 46506),  # postcodes that went via a float
        ("12", "bigint", 12),
        ("abc", "int", None),  # ANSI mode would raise; we want NULL
        (None, "bigint", None),
    ],
)
def test_cast_is_tolerant(spark, value, dtype, expected):
    df = spark.createDataFrame([(value,)], "v string")
    assert cast_columns(df, {"v": dtype}).first()["v"] == expected


def test_strip_float_suffix(spark):
    df = spark.createDataFrame([("46506.0", "521.0", "no")], "postcode string, number string, other string")
    row = strip_float_suffix(df, ["postcode", "number"]).first()
    assert (row["postcode"], row["number"], row["other"]) == ("46506", "521", "no")


def test_add_derived_applies_in_order(spark):
    df = spark.createDataFrame([(2, 3)], "a int, b int")
    row = add_derived(df, {"total": "a * b", "doubled": "total * 2"}).first()
    assert (row["total"], row["doubled"]) == (6, 12)


def test_parse_json_column_gives_a_typed_struct(spark):
    df = spark.createDataFrame([('{"k": 1}',)], "meta string")
    parsed = parse_json_column(df, "meta", "struct<k:int>")
    assert parsed.first()["meta"]["k"] == 1


def test_explode_array_creates_one_row_per_element_with_positions(spark):
    df = spark.createDataFrame([(1, '[{"id":"A","qty":"2"},{"id":"B","qty":"5"}]'), (2, "[]")], "order_number int, items string")
    parsed = parse_json_column(df, "items", "array<struct<id:string,qty:string>>")
    exploded = explode_array(parsed, "items", alias="p", position_column="line_number")
    result = add_derived(exploded, {"product_id": "p.id", "quantity": "try_cast(p.qty AS INT)"})
    rows = sorted((r["order_number"], r["line_number"], r["product_id"], r["quantity"]) for r in result.collect())

    assert rows == [(1, 1, "A", 2), (1, 2, "B", 5)], "one row per element, 1-based positions"
    assert "items" not in exploded.columns, "the array is consumed"
    assert not any(r["order_number"] == 2 for r in result.collect()), "an empty array produces no rows"


def test_drop_columns_ignores_missing(spark):
    df = spark.createDataFrame([(1, 2)], "a int, b int")
    assert drop_columns(df, ["b", "nope"]).columns == ["a"]
