"""Run every Gold .sql file against fixture Silver tables on local Spark.

This is the most valuable test in the suite: the Gold layer is pure SQL, and SQL is
where the expensive mistakes hide — a wrong join that quietly drops revenue, a
duplicate surrogate key, a fact pointing at a dimension row that does not exist.

The fixtures below *are* the Silver contract. If a Silver notebook stops producing
one of these columns, this test fails and tells you which product broke.
"""

import re
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

SQL_STATEMENT = re.compile(r"^\s*CREATE\s+OR\s+REPLACE\s+(TABLE|VIEW)\s+(\S+)\s+AS\s+(.*)$", re.IGNORECASE | re.DOTALL)


def sql_files(root):
    return sorted((root / "src" / "gold" / "sql").glob("*.sql"))


def render(sql: str) -> str:
    return sql.replace("${catalog}", "spark_catalog").replace("${silver}", "silver").replace("${gold}", "gold")


def frame(spark, rows, schema: str):
    """createDataFrame, but money can be written as 100.00 instead of Decimal('100.00').

    Spark refuses a Python float for a decimal column, and spelling Decimal() around
    every amount below would bury the data the fixtures are trying to show.
    """
    df = spark.createDataFrame(rows, re.sub(r"decimal\(\d+,\s*\d+\)", "double", schema, flags=re.IGNORECASE))
    for name, dtype in re.findall(r"(\w+)\s+(decimal\(\d+,\s*\d+\))", schema, re.IGNORECASE):
        df = df.withColumn(name, F.col(name).cast(dtype))
    return df


def split_statement(text: str):
    """Return (kind, target, body) — the runner executes the whole statement; the test runs the body."""
    without_comments = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    match = SQL_STATEMENT.match(without_comments.strip().rstrip(";"))
    return match.groups() if match else (None, None, None)


# --------------------------------------------------------------------------- #
# Fixtures: what Silver promises Gold
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def silver(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    now, high = datetime(2026, 9, 1), datetime(9999, 12, 31)
    customers = frame(
        spark,
        [
            # customer 1001 has two versions: the old one closed, the new one current
            (1001, "DOE, JANE", "individual", "JANE", "DOE", "CA", "OLDTOWN", "90000", "MAIN ST", "10", None, "California", "LA",
             -118.2, 34.0, "CA, 90000", 3, "Platinum", 12, True, now, None, datetime(2026, 1, 1), now, False),
            (1001, "DOE, JANE", "individual", "JANE", "DOE", "CA", "NEWTOWN", "90001", "MAIN ST", "10", None, "California", "LA",
             -118.2, 34.0, "CA, 90001", 3, "Platinum", 12, True, now, None, now, high, True),
            (1002, "acme trading ltd", "organisation", None, None, "NY", "NEW YORK", "10001", "BROAD ST", "1", None, "NY", "NY",
             -74.0, 40.7, "NY, 10001", 0, "Bronze", 3, True, now, None, now, high, True),
        ],
        "customer_id bigint, customer_name string, customer_type string, first_name string, last_name string, state string, city string, "
        "postcode string, street string, number string, unit string, region string, district string, lon double, lat double, "
        "ship_to_address string, loyalty_segment int, loyalty_segment_name string, units_purchased int, is_active boolean, "
        "source_valid_from_ts timestamp, source_valid_to_ts timestamp, effective_from timestamp, effective_to timestamp, is_current boolean",
    )
    customers.write.mode("overwrite").saveAsTable("spark_catalog.silver.customers")

    lines = frame(
        spark,
        [
            # order 1: two lines, one with a 5% promotion
            (1, 1, "docB", 1001, datetime(2026, 1, 15, 10), date(2026, 1, 15), "P1", "Ramsung Tab", 2, 100.00, "USD", "pcs", "1", 0.05, 2, 200.00, 10.00, 190.00),
            (1, 2, "docB", 1001, datetime(2026, 1, 15, 10), date(2026, 1, 15), "P2", "Sioneer Receiver", 1, 50.00, "USD", "pcs", "NONE", 0.0, None, 50.00, 0.00, 50.00),
            # order 2: customer 9999 is not in the CRM -> must resolve to the Unknown member
            (2, 1, "docC", 9999, datetime(2026, 1, 16, 9), date(2026, 1, 16), "P1", "Ramsung Tab", 3, 100.00, "USD", "pcs", "NONE", 0.0, None, 300.00, 0.00, 300.00),
            # order 3: no timestamp in the source -> NULL date key, still counted
            (3, 1, "docD", 1002, None, None, "P3", "Mogitech Wheel", 1, 80.00, "USD", "pcs", "NONE", 0.0, None, 80.00, 0.00, 80.00),
        ],
        "order_number bigint, line_number int, source_document_id string, customer_id bigint, order_ts timestamp, order_date date, "
        "product_id string, product_name string, quantity int, unit_price decimal(12,2), currency string, unit string, "
        "promo_id string, promo_discount_rate double, promo_quantity int, gross_amount decimal(14,2), discount_amount decimal(14,2), net_amount decimal(14,2)",
    )
    lines.write.mode("overwrite").saveAsTable("spark_catalog.silver.sales_order_lines")

    orders = frame(
        spark,
        [
            (1, "docB", 1001, "DOE, JANE", 2, datetime(2026, 1, 15, 10), date(2026, 1, 15), 2, True, 2, 12, 250.00, now, high, True),
            (2, "docC", 9999, "GHOST, CUSTOMER", 1, datetime(2026, 1, 16, 9), date(2026, 1, 16), 1, False, 1, 3, 300.00, now, high, True),
            (3, "docD", 1002, "acme trading ltd", 1, None, None, 1, False, 0, 0, 80.00, now, high, True),
        ],
        "order_number bigint, source_document_id string, customer_id bigint, customer_name string, number_of_line_items int, "
        "order_ts timestamp, order_date date, line_item_count int, has_promotion boolean, clicked_item_count int, click_count bigint, "
        "order_gross_amount decimal(14,2), effective_from timestamp, effective_to timestamp, is_current boolean",
    )
    orders.write.mode("overwrite").saveAsTable("spark_catalog.silver.sales_orders")

    sales = frame(
        spark,
        [
            ("hash1", 1001, "DOE, JANE", "Something else", "Ramsung", date(2026, 1, 20), "P1", "Ramsung Tab", 100.00, 2, "USD", 200.00),
            ("hash2", 1002, "acme trading ltd", "Other", None, date(2026, 1, 21), "P4", "Zamaha Amp", 40.00, 1, "USD", 40.00),
        ],
        "sale_id string, customer_id bigint, customer_name string, listed_product_name string, brand string, order_date date, "
        "product_id string, product_name string, unit_price decimal(12,2), quantity int, currency string, total_amount decimal(14,2)",
    )
    sales.write.mode("overwrite").saveAsTable("spark_catalog.silver.sales")
    return spark


@pytest.fixture(scope="module")
def gold(spark, silver, project_root):
    """Run the .sql files in filename order, exactly as build_gold does."""
    for path in sql_files(project_root):
        if path.name.endswith(".optional.sql"):
            continue  # metric views need Unity Catalog
        kind, target, body = split_statement(render(path.read_text()))
        assert body, f"{path.name} is not a single CREATE OR REPLACE statement"
        spark.sql(body).write.mode("overwrite").saveAsTable(target)
    return spark


# --------------------------------------------------------------------------- #
# Contract: what every .sql file must look like
# --------------------------------------------------------------------------- #
def test_every_sql_file_is_one_create_or_replace_statement(project_root):
    files = sql_files(project_root)
    assert len(files) >= 10
    for path in files:
        if path.name.endswith(".optional.sql"):
            # Metric views have their own DDL shape: ... VIEW <name> WITH METRICS LANGUAGE YAML AS $$ ... $$
            header = re.search(r"CREATE\s+OR\s+REPLACE\s+VIEW\s+(\S+)\s+WITH\s+METRICS\s+LANGUAGE\s+YAML\s+AS", path.read_text(), re.IGNORECASE)
            assert header, f"{path.name} must be a single CREATE OR REPLACE VIEW ... WITH METRICS statement"
            assert header.group(1).startswith("${catalog}.${gold}.")
            continue
        kind, target, body = split_statement(path.read_text())
        assert kind, f"{path.name} must be a single CREATE OR REPLACE TABLE|VIEW statement"
        assert target.startswith("${catalog}.${gold}."), f"{path.name} must write into ${{catalog}}.${{gold}}"


def test_sql_files_never_hard_code_the_catalog(project_root):
    """A literal catalog name would break deployment to any other workspace."""
    for path in sql_files(project_root):
        text = path.read_text()
        assert "retaildataplatform." not in text, f"{path.name}: use ${{catalog}} instead of a literal catalog name"


def test_sql_files_are_numbered_so_dependencies_run_in_order(project_root):
    names = [p.name for p in sql_files(project_root)]
    assert all(re.match(r"^\d{2}_", n) for n in names), "filenames carry the build order"
    order = {re.sub(r"^\d+_|\.optional|\.sql$", "", n): i for i, n in enumerate(names)}
    assert order["dim_customer"] < order["fact_sales_order_line"], "dimensions before facts"
    assert order["dim_promotion"] < order["fact_sales_order_line"]
    assert order["fact_sales_order_line"] < order["fact_sales_order"], "the header fact aggregates the line fact"
    assert order["fact_sales_order_line"] < order["sales_order_lines_obt"], "views after the facts they read"


# --------------------------------------------------------------------------- #
# Behaviour: does the star actually work?
# --------------------------------------------------------------------------- #
def test_primary_keys_are_unique_and_not_null(gold):
    for table, key in [
        ("dim_date", "date_key"),
        ("dim_customer", "customer_sk"),
        ("dim_product", "product_sk"),
        ("dim_promotion", "promo_id"),
        ("fact_sales_order_line", "order_line_id"),
        ("fact_sales_order", "order_number"),
        ("fact_pos_sale", "sale_id"),
    ]:
        df = gold.table(f"spark_catalog.gold.{table}")
        assert df.filter(f"`{key}` IS NULL").count() == 0, f"{table}.{key} has NULLs"
        assert df.groupBy(key).count().filter("count > 1").count() == 0, f"{table}.{key} is not unique"


def test_no_orphan_foreign_keys(gold):
    for fact, column, dim, key in [
        ("fact_sales_order_line", "customer_sk", "dim_customer", "customer_sk"),
        ("fact_sales_order_line", "product_sk", "dim_product", "product_sk"),
        ("fact_sales_order_line", "promo_id", "dim_promotion", "promo_id"),
        ("fact_sales_order_line", "order_date_key", "dim_date", "date_key"),
        ("fact_pos_sale", "customer_sk", "dim_customer", "customer_sk"),
        ("fact_pos_sale", "product_sk", "dim_product", "product_sk"),
    ]:
        orphans = gold.sql(
            f"""
            SELECT count(*) FROM spark_catalog.gold.{fact} f
            LEFT ANTI JOIN spark_catalog.gold.{dim} d ON d.{key} = f.{column}
            WHERE f.{column} IS NOT NULL
            """
        ).first()[0]
        assert orphans == 0, f"{fact}.{column} -> {dim}: {orphans} orphan rows"


def test_unknown_members_catch_missing_dimension_rows(gold):
    lines = gold.table("spark_catalog.gold.fact_sales_order_line")
    assert lines.count() == 4, "no line is dropped, even when its customer is unknown"
    assert lines.filter("customer_sk = -1").count() == 1, "customer 9999 resolves to the Unknown member"
    assert lines.filter("order_date_key IS NULL").count() == 1, "an order without a timestamp keeps a NULL date key"
    assert gold.table("spark_catalog.gold.dim_customer").filter("customer_sk = -1").count() == 1
    assert gold.table("spark_catalog.gold.dim_product").filter("product_sk = -1").count() == 1


def test_revenue_reconciles_from_silver_through_to_the_header_fact(gold):
    row = gold.sql(
        """
        SELECT (SELECT sum(net_amount)   FROM spark_catalog.gold.fact_sales_order_line) AS lines_net,
               (SELECT sum(net_amount)   FROM spark_catalog.gold.fact_sales_order)      AS headers_net,
               (SELECT sum(gross_amount) FROM spark_catalog.gold.fact_sales_order_line) AS lines_gross,
               (SELECT sum(order_gross_amount) FROM spark_catalog.silver.sales_orders WHERE is_current) AS silver_gross
        """
    ).first()
    assert row["lines_net"] == row["headers_net"], "line totals must roll up to header totals"
    assert row["lines_gross"] == row["silver_gross"], "no revenue created or lost between Silver and Gold"
    assert row["lines_net"] == 620.00, "200 - 10 discount + 50 + 300 + 80"


def test_dim_customer_keeps_versions_and_marks_the_current_one(gold):
    dim = gold.table("spark_catalog.gold.dim_customer")
    versions = dim.filter("customer_id = 1001")
    assert versions.count() == 2, "SCD2 history survives into Gold"
    assert versions.filter("is_current").count() == 1
    assert versions.filter("is_current").first()["city"] == "NEWTOWN"
    assert gold.table("spark_catalog.gold.customers_current").count() == 2, "current customers, excluding the Unknown member"


def test_dim_product_derives_brand_and_records_how(gold):
    brands = {r["product_id"]: (r["brand"], r["brand_source"]) for r in gold.table("spark_catalog.gold.dim_product").collect()}
    assert brands["P1"] == ("Ramsung", "pos_export"), "the POS export is the most trusted source of brand"
    assert brands["P3"] == ("Mogitech", "name_prefix"), "fall back to the leading word of the name"
    assert brands[None] == ("Unknown", "unknown"), "the Unknown member"


def test_dim_promotion_labels_every_code_including_none(gold):
    promotions = {r["promo_id"]: (r["promotion_name"], r["discount_rate"]) for r in gold.table("spark_catalog.gold.dim_promotion").collect()}
    assert promotions["NONE"][0] == "No promotion" and promotions["NONE"][1] == 0.0
    assert "5% off" in promotions["1"][0]


def test_one_big_table_view_loses_no_lines(gold):
    obt = gold.table("spark_catalog.gold.sales_order_lines_obt")
    assert obt.count() == gold.table("spark_catalog.gold.fact_sales_order_line").count(), "inner joins are safe because of the Unknown members"
    assert {"customer_name", "brand", "promotion_name", "net_amount", "year_month"} <= set(obt.columns)
