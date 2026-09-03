"""End-to-end checks that need Delta + Unity Catalog. Run inside Databricks:

    %pip install pytest
    import pytest, sys; sys.path.insert(0, "<repo root>")
    pytest.main(["-m", "integration", "<repo root>/tests/test_integration_databricks.py", "-q"])

They exercise the SCD merges and idempotent Bronze writes against a scratch schema and drop it afterwards.
"""

import uuid
from datetime import date

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def scratch(spark):
    catalog, schema = "retaildataplatform", f"test_{uuid.uuid4().hex[:8]}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    yield catalog, schema
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")


def test_scd2_merge_tracks_history_and_is_idempotent(spark, scratch):
    from retail_platform.scd import merge_type_2, row_hash

    catalog, schema = scratch
    target = f"`{catalog}`.`{schema}`.`dim`"
    day1 = row_hash(spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string"), ["id", "v"])
    merge_type_2(spark, day1, target, ["id"])
    merge_type_2(spark, day1, target, ["id"])  # identical batch: no new versions
    assert spark.table(target).count() == 2

    day2 = row_hash(spark.createDataFrame([(1, "a2"), (3, "c")], "id int, v string"), ["id", "v"])
    merge_type_2(spark, day2, target, ["id"], detect_deletes=True)
    rows = spark.table(target).orderBy("id", "effective_from").collect()
    by_id = {}
    for r in rows:
        by_id.setdefault(r["id"], []).append(r)
    assert [r["is_current"] for r in by_id[1]] == [False, True]
    assert by_id[2][0]["is_current"] is False, "key missing from the full extract is closed"
    assert by_id[3][0]["is_current"] is True


def test_bronze_write_is_idempotent_per_load_date(spark, scratch):
    from retail_platform.bronze import write_idempotent

    catalog, schema = scratch
    df = spark.createDataFrame([(1, date(2026, 9, 1)), (2, date(2026, 9, 1))], "id int, _load_date date")
    write_idempotent(spark, df, catalog, schema, "b", "2026-09-01")
    write_idempotent(spark, df, catalog, schema, "b", "2026-09-01")
    other = spark.createDataFrame([(3, date(2026, 9, 2))], "id int, _load_date date")
    write_idempotent(spark, other, catalog, schema, "b", "2026-09-02")
    assert spark.table(f"`{catalog}`.`{schema}`.`b`").count() == 3
