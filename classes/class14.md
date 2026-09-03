# Class 14 — Building the dimensions

## Objectives

* Generate `dim_date` from a date range with `sequence` + `explode`.
* Build `dim_customer` from Silver SCD2 with a surrogate key and an Unknown member.
* Build `dim_product` from two channels: explode `ordered_products`, union with POS, derive brand with fallbacks.
* Write each as a Delta table with an overwrite (full rebuild).

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap the star drawing |
| 10–30 | `dim_date` |
| 30–55 | `dim_customer` |
| 55–90 | `dim_product` (explode, union, coalesce chain) |
| 90–100 | Homework |

## `dim_date` (20 min)

v1 in SQL to show the trick:

```sql
SELECT explode(sequence(to_date('2019-01-01'), to_date('2019-01-10'), interval 1 day)) AS date;
```

v2 in PySpark, building the attributes one by one and explaining each:

```python
from pyspark.sql import functions as F

def date_dimension(start_date, end_date):
    df = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date")
    return df.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.concat(F.year("date"), F.lit("-Q"), F.quarter("date")).alias("year_quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.weekofyear("date").alias("iso_week"),
        F.dayofmonth("date").alias("day_of_month"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        F.date_trunc("month", "date").cast("date").alias("first_day_of_month"),
        F.last_day("date").alias("last_day_of_month"),
    )

date_dimension("2015-01-01", "2030-12-31").write.format("delta").mode("overwrite").saveAsTable("retaildataplatform.gold.dim_date")
```

Ask: why 2015–2030 and not "min to max order date"? (Stable keys; BI date tables must
cover future planning dates.)

## `dim_customer` (25 min)

```sql
CREATE OR REPLACE TABLE retaildataplatform.gold.dim_customer AS
SELECT xxhash64(CAST(customer_id AS STRING), CAST(effective_from AS STRING)) AS customer_sk,
       customer_id, customer_name, customer_type, first_name, last_name, state, city, postcode,
       street, number, unit, region, district, lon, lat, ship_to_address,
       loyalty_segment, loyalty_segment_name, units_purchased, is_active,
       source_valid_from_ts AS crm_valid_from, source_valid_to_ts AS crm_valid_to,
       effective_from, effective_to, is_current
FROM retaildataplatform.silver.customers
UNION ALL
SELECT -1, NULL, 'Unknown', 'unknown', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, NULL, NULL, 'Unknown', NULL, NULL, NULL, NULL,
       TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', true;
```

Three teaching points: `xxhash64` gives a 64-bit integer surrogate (fast joins); the
`UNION ALL` row is the Unknown member (count the columns twice — the most common bug
is a column-count mismatch); we deliberately drop `_row_hash` and lineage columns
from Gold. Check:

```sql
SELECT count(*), count(DISTINCT customer_sk), sum(CASE WHEN is_current THEN 1 END) FROM retaildataplatform.gold.dim_customer;
```

## `dim_product` (35 min)

There is no product master, so the dimension is *derived*. Build the query in stages,
each as its own cell with a `display`:

Stage 1 — products from web orders (explode):

```sql
SELECT p.id AS product_id, p.name AS product_name
FROM retaildataplatform.silver.sales_orders
LATERAL VIEW explode(ordered_products) t AS p
WHERE is_current = true
```

`LATERAL VIEW explode` turns one row with an array of 3 products into 3 rows. Show the
count before and after.

Stage 2 — products from POS (`silver.sales`) with brand.

Stage 3 — union, group by `product_id`, keep the most common name and count `times_sold`.

Stage 4 — brand with fallbacks, explained as a `coalesce` chain: POS export brand →
brand word found in the name (`ILIKE '%Ramsung%'` against the distinct POS brands) →
first alphabetic word of the name (`regexp_extract(name, '^([A-Za-z]{3,})', 1)`) →
`'Unknown'`. Record which fallback fired in `brand_source` so analysts can judge
confidence. Then the Unknown member `-1`.

Paste the final statement from `src/config/gold.json` (`dim_product.sql`) and run it;
students compare with their stage-by-stage version. Check brand coverage:

```sql
SELECT brand, brand_source, count(*) FROM retaildataplatform.gold.dim_product GROUP BY 1, 2 ORDER BY 1;
```

## Homework

1. Add `is_holiday` to `dim_date` for three fixed dates using `CASE WHEN`.
2. Find the products whose name differs between web orders and POS (`max(name)` hid it). How would you choose?
3. Write the `customers_current` view (`WHERE is_current AND customer_sk <> -1`).

## Common problems

* `UNION ALL` column count/type mismatch → count columns, cast literals (`-1` vs bigint is fine; `TIMESTAMP'...'` literals for the dates).
* `explode` on a NULL array yields no rows — use `explode_outer` if you must keep the parent; here dropping is correct.
* Students write `CREATE TABLE` without `OR REPLACE` and cannot rerun — Gold is a full rebuild every run by design.
