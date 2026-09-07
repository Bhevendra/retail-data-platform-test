"""Delta-only tests: SCD merges and idempotent writes. Run these inside Databricks.

    %pip install pytest
    import pytest, sys; sys.path.insert(0, "<repo root>")
    pytest.main(["-m", "integration", "<repo root>/tests/test_integration_databricks.py", "-q"])

They create a scratch schema and drop it afterwards.
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


def test_scd2_keeps_history_and_ignores_identical_batches(spark, scratch):
    from common_utils.scd import row_hash, scd2_merge

    catalog, schema = scratch
    target = f"`{catalog}`.`{schema}`.`dim`"

    day1 = row_hash(spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string"), ["id", "v"])
    scd2_merge(spark, day1, target, ["id"])
    scd2_merge(spark, day1, target, ["id"])
    assert spark.table(target).count() == 2, "an identical batch creates no new versions"

    day2 = row_hash(spark.createDataFrame([(1, "a2"), (3, "c")], "id int, v string"), ["id", "v"])
    scd2_merge(spark, day2, target, ["id"], detect_deletes=True)

    rows = {}
    for row in spark.table(target).orderBy("id", "effective_from").collect():
        rows.setdefault(row["id"], []).append(row)
    assert [r["is_current"] for r in rows[1]] == [False, True], "the changed key gets a second version"
    assert rows[2][0]["is_current"] is False, "a key missing from a full extract is closed"
    assert rows[3][0]["is_current"] is True


def test_scd1_updates_changed_rows_only(spark, scratch):
    from common_utils.scd import row_hash, scd1_merge

    catalog, schema = scratch
    target = f"`{catalog}`.`{schema}`.`facts`"

    first = row_hash(spark.createDataFrame([(1, 10), (2, 20)], "id int, amount int"), ["id", "amount"])
    scd1_merge(spark, first, target, ["id"])
    second = row_hash(spark.createDataFrame([(1, 10), (2, 99), (3, 30)], "id int, amount int"), ["id", "amount"])
    scd1_merge(spark, second, target, ["id"])

    amounts = {r["id"]: r["amount"] for r in spark.table(target).collect()}
    assert amounts == {1: 10, 2: 99, 3: 30}


def test_bronze_writes_are_idempotent_per_load_date(spark, scratch):
    from common_utils.writers import write_idempotent

    catalog, schema = scratch
    df = spark.createDataFrame([(1, date(2026, 9, 1)), (2, date(2026, 9, 1))], "id int, _load_date date")
    write_idempotent(spark, df, catalog, schema, "bronze_demo", "2026-09-01")
    write_idempotent(spark, df, catalog, schema, "bronze_demo", "2026-09-01")

    other = spark.createDataFrame([(3, date(2026, 9, 2))], "id int, _load_date date")
    write_idempotent(spark, other, catalog, schema, "bronze_demo", "2026-09-02")

    table = spark.table(f"`{catalog}`.`{schema}`.`bronze_demo`")
    assert table.count() == 3, "re-running a day replaces it; other days are untouched"
    assert table.filter("_load_date = '2026-09-01'").count() == 2
