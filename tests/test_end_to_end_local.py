"""Silver -> Gold end-to-end on local Spark with synthetic fixtures shaped like the real sources.

Exercises every transformation in silver.json and every SQL product in gold.json (metric
views excepted - they need Unity Catalog), then checks keys, referential integrity and
that revenue reconciles across line / header / Silver.
"""

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from common_utils.config import load_gold_config, load_silver_config
from common_utils.gold import date_dimension, render_sql
from common_utils.quality import evaluate_rules
from common_utils.runtime import RunContext
from common_utils.silver import prepare

FIXTURES = Path(__file__).parent / "fixtures"


def _bronze_like(spark, path: Path):
    df = spark.read.option("header", "true").option("inferSchema", "true").option("escape", '"').option("multiLine", "true").csv(str(path))
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit("bronze-run"))
        .withColumn("_load_date", F.lit("2026-09-03").cast("date"))
        .withColumn("_source_system", F.lit("fixture"))
        .withColumn("_source_file", F.lit(str(path)))
        .withColumn("last_update_ts", F.current_timestamp())
        .withColumn("file_path", F.lit(str(path)))
    )


@pytest.fixture(scope="module")
def star(spark):
    ctx = RunContext(environment="test", catalog="spark_catalog", secret_scope="s", run_date=date(2026, 9, 3), run_id="r1")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    silver = load_silver_config()
    for entity in silver.entities:
        out = prepare(_bronze_like(spark, FIXTURES / f"{entity.source_table}.csv"), entity, ctx)
        if entity.scd_type == 2:  # emulate what merge_type_2 adds on first load
            out = out.withColumn("effective_from", F.current_timestamp()).withColumn("effective_to", F.lit("9999-12-31").cast("timestamp")).withColumn("is_current", F.lit(True))
        out.write.mode("overwrite").saveAsTable(f"spark_catalog.silver.{entity.target_table}")
    gold = load_gold_config()
    built = []
    for product in gold.ordered_products():
        if product.type == "date_dimension":
            date_dimension(spark, "2019-01-01", "2020-12-31").write.mode("overwrite").saveAsTable(f"spark_catalog.gold.{product.name}")
        elif product.type == "table":
            spark.sql(render_sql(product.sql, "spark_catalog", "silver", "gold")).write.mode("overwrite").saveAsTable(f"spark_catalog.gold.{product.name}")
        elif product.type == "view":
            spark.sql(f"CREATE OR REPLACE VIEW spark_catalog.gold.{product.name} AS {render_sql(product.sql, 'spark_catalog', 'silver', 'gold')}")
        else:
            continue
        built.append(product)
    return gold, built


def test_silver_transformations_on_fixtures(spark, star):
    orders = spark.table("spark_catalog.silver.sales_orders")
    assert orders.count() == 8, "re-emitted order version collapses to one row per order_number"
    latest = orders.filter("order_number = 317568001").first()
    assert latest["number_of_line_items"] == 3 and latest["source_document_id"].endswith("9" * 22), "latest Cosmos document wins"
    assert "ordered_products" not in orders.columns, "nested arrays are flattened into child tables, not kept on the header"
    assert orders.filter("order_ts IS NULL").count() == 1 and orders.filter("has_promotion").count() >= 1

    lines = spark.table("spark_catalog.silver.sales_order_lines")
    assert lines.count() == orders.agg(F.sum("line_item_count")).first()[0], "one line row per element of ordered_products"
    assert lines.groupBy("order_number", "line_number").count().filter("count > 1").count() == 0
    assert lines.filter("order_number = 317568001").count() == latest["line_item_count"], "lines come from the latest version of a re-emitted order"
    assert lines.filter("order_number = 317568001").select("source_document_id").distinct().first()[0].endswith("9" * 22)
    assert lines.filter("promo_id <> 'NONE'").count() > 0 and lines.filter("discount_amount > net_amount").count() == 0
    silver_recon = lines.groupBy("order_number").agg(F.sum("gross_amount").alias("g")).join(orders, "order_number").filter("g <> order_gross_amount").count()
    assert silver_recon == 0, "line gross reconciles with the header gross at Silver"
    clicks = spark.table("spark_catalog.silver.sales_order_clicks")
    assert clicks.count() > 0 and clicks.groupBy("order_number", "product_id").count().filter("count > 1").count() == 0

    sales = spark.table("spark_catalog.silver.sales")
    assert sales.count() == 6, "exact duplicate POS row collapses"
    broken = sales.filter("product_id = 'P3'").first()
    assert broken["product_name"].endswith('English"') and broken["unit_price"] is not None, "regex parsing survives unescaped quotes"
    assert sales.filter("total_amount <> unit_price * quantity").count() == 0

    customers = spark.table("spark_catalog.silver.customers")
    assert customers.count() == 4 and customers.filter("customer_id = 1003").first()["is_active"] is True, "latest CRM version per customer"
    row = customers.filter("customer_id = 1001").first()
    assert (row["last_name"], row["first_name"], row["customer_type"], row["postcode"], row["number"]) == ("DOE", "JANE", "individual", "90000", "10")
    assert customers.filter("customer_id = 1002").first()["customer_type"] == "organisation"
    assert customers.filter("customer_id = 1004").first()["is_active"] is False


def test_gold_keys_and_referential_integrity(spark, star):
    gold, built = star
    for product in built:
        if product.type not in {"table", "date_dimension"}:
            continue
        table = spark.table(f"spark_catalog.gold.{product.name}")
        failed = [(r.rule.name, r.failed_rows) for r in evaluate_rules(table, product.quality_rules) if r.blocking]
        assert not failed, f"{product.name}: blocking rules failed {failed}"
        for column in product.primary_key:
            assert table.filter(F.col(column).isNull()).count() == 0, f"{product.name}.{column} has NULLs"
        assert table.groupBy(*product.primary_key).count().filter("count > 1").count() == 0, f"{product.name}: primary key not unique"
        for fk in product.foreign_keys:
            ref = spark.table(f"spark_catalog.gold.{fk.references}")
            condition = [table[a] == ref[b] for a, b in zip(fk.columns, fk.referenced_columns, strict=True)]
            orphans = table.join(ref, condition, "left_anti").filter(F.col(fk.columns[0]).isNotNull()).count()
            assert orphans == 0, f"{product.name}.{fk.columns} -> {fk.references}: {orphans} orphan rows"


def test_gold_unknown_members_and_reconciliation(spark, star):
    lines = spark.table("spark_catalog.gold.fact_sales_order_line")
    headers = spark.table("spark_catalog.gold.fact_sales_order")
    assert lines.filter("customer_sk = -1").count() > 0, "orders for customer 9999 resolve to the Unknown member"
    assert lines.filter("order_date_key IS NULL").count() >= 1, "orders without timestamp keep a NULL date key"
    assert lines.filter("promo_discount_rate > 0").count() > 0 and lines.filter("discount_amount > net_amount").count() == 0

    line_total = lines.agg(F.sum("net_amount")).first()[0]
    header_total = headers.agg(F.sum("net_amount")).first()[0]
    silver_gross = spark.table("spark_catalog.silver.sales_orders").agg(F.sum("order_gross_amount")).first()[0]
    fact_gross = lines.agg(F.sum("gross_amount")).first()[0]
    assert line_total == header_total and silver_gross == fact_gross

    promotions = {r["promo_id"]: r["promotion_name"] for r in spark.table("spark_catalog.gold.dim_promotion").collect()}
    assert promotions["NONE"] == "No promotion" and any("% off" in name for name in promotions.values())

    products = spark.table("spark_catalog.gold.dim_product")
    by_id = {r["product_id"]: (r["brand"], r["brand_source"]) for r in products.collect()}
    assert by_id["P1"] == ("Ramsung", "pos_export") and by_id["P4"] == ("Mogitech", "name_prefix") and by_id[None] == ("Unknown", "unknown")
    assert spark.table("spark_catalog.gold.sales_order_lines_obt").count() == lines.count()
    assert "customer_name" in spark.table("spark_catalog.gold.pos_sales_obt").columns
