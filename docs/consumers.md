# Guide for BI and AI consumers

Everything you should query lives in **`retaildataplatform.gold`**. Bronze and Silver
are engineering layers and may change shape; Gold is the contract.

## The model

```
                 dim_date ─────────────┐
                                       │ order_date_key / sale_date_key
 dim_customer ──┐                      │
 (SCD2, -1 = Unknown)                  ▼
                ├──> fact_sales_order_line  (web shop, grain: order x line)   ──> mv_web_sales
                ├──> fact_sales_order       (web shop, grain: order)
 dim_product ───┤
 (-1 = Unknown) └──> fact_pos_sale          (stores, grain: product sold)     ──> mv_pos_sales

 views: customers_current, sales_order_lines_obt, pos_sales_obt
```

Web-shop orders and point-of-sale transactions are **separate channels** with no
shared transaction id; compare them by customer, product, brand or date, never join
them row to row. Revenue measures: `fact_sales_order_line.net_amount` (after promotion
discount) and `fact_pos_sale.total_amount`.

* **Dimensions** (`dim_*`) describe *who / what / when*. They have an integer or
  hash surrogate key (`*_sk`, or `date_key` for dates) declared as PRIMARY KEY.
* **Facts** (`fact_*`) are events or transactions at a declared grain, with FOREIGN
  KEYs to the dimensions. Join on the surrogate keys.
* **Current views** (`*_current`) give the latest version of SCD2 dimensions when you
  do not need history.
* **One-big-table views** (`*_obt`) pre-join a fact with its dimensions for ad-hoc
  analysis, notebooks and LLM agents (Genie) that prefer a single wide table.
* **Metric views** (`mv_*`) hold governed measures (e.g. revenue, order count) so every
  dashboard and agent computes them the same way.

Every table and column carries a `COMMENT`; `DESCRIBE TABLE EXTENDED` or the Catalog
Explorer shows them. The full list is in `docs/data-dictionary.md`.

## Example questions and the query that answers them

```sql
-- Net web revenue and orders by month and brand
SELECT d.year_month, p.brand, sum(f.net_amount) AS net_revenue, count(DISTINCT f.order_number) AS orders
FROM retaildataplatform.gold.fact_sales_order_line f
JOIN retaildataplatform.gold.dim_product p ON p.product_sk = f.product_sk
JOIN retaildataplatform.gold.dim_date d ON d.date_key = f.order_date_key
GROUP BY 1, 2 ORDER BY 1, 2;

-- Same answer through the metric view (no joins to get wrong)
SELECT year_month, brand, MEASURE(net_revenue) AS net_revenue, MEASURE(orders) AS orders
FROM retaildataplatform.gold.mv_web_sales GROUP BY 1, 2 ORDER BY 1, 2;

-- Top loyalty segments by average order value
SELECT loyalty_segment, MEASURE(average_order_value) FROM retaildataplatform.gold.mv_web_sales GROUP BY 1;

-- Customers who bought in store and online
SELECT c.customer_id, c.customer_name
FROM retaildataplatform.gold.customers_current c
WHERE EXISTS (SELECT 1 FROM retaildataplatform.gold.fact_pos_sale s WHERE s.customer_sk = c.customer_sk)
  AND EXISTS (SELECT 1 FROM retaildataplatform.gold.fact_sales_order o WHERE o.customer_sk = c.customer_sk);
```

## Known data caveats (documented, not hidden)

* About 1% of web orders have no timestamp: they carry `order_date_key = NULL` and are
  excluded from date-based charts but included in totals.
* A web order can be re-emitted by the source when lines are added; Gold shows the
  latest version, Silver keeps the earlier states (`is_current = false`).
* The POS export has no transaction id; `sale_id` is a hash of the row, so identical
  duplicate rows in the export are counted once.
* Product brand is derived (see `dim_product.brand_source`); there is no product master.
* Customer `region` / `district` are inconsistently coded upstream; use `state` and `city`.

## Power BI / Tableau

* Connect through a SQL warehouse to catalog `retaildataplatform`, schema `gold`.
* Power BI detects relationships from the PK/FK constraints; keep the star: facts
  many-to-one to dimensions, single direction.
* Use `dim_date` as the date table (mark `date` as the date column) for time
  intelligence.
* SCD2 dimensions: for "as of today" reports use `*_current`; for "as it was at the
  time" reports the fact already points at the version valid at the event, so
  `fact -> dim_*` on `*_sk` is correct without extra filters.
* Freshness tile: `SELECT max(finished_at) FROM retaildataplatform.ops.pipeline_runs WHERE layer='gold' AND status='SUCCEEDED'`.

## AI / ML engineers

* Column comments, PK/FK constraints and metric views are read by Databricks Genie and
  the Assistant; ask questions against the `gold` schema or a Genie space built on it.
* For feature engineering read the `*_obt` views or the facts directly; all tables have
  **change data feed** enabled (`table_changes('retaildataplatform.gold.fact_sales_order_line', <version>)`)
  so incremental feature refreshes are cheap.
* Lineage to raw bytes: `_run_id` on every row joins to `ops.pipeline_runs`; Silver keeps
  `_load_date`, Bronze keeps `_source_file` pointing at the immutable raw volume.
* PII columns are tagged `classification = pii` in Unity Catalog. Query
  `system.information_schema.column_tags` to find them and apply masking or exclusion
  in your feature pipelines.

## Service levels

| Item | Value |
| --- | --- |
| Refresh | Daily, 05:00 UTC (Gold ready typically by 05:30 UTC) |
| Grain and keys | Declared per table in `docs/data-dictionary.md` |
| Quality gate | `error` rules must pass (or rows are quarantined) before data reaches Gold |
| Breaking changes | Announced via PR to `src/config/gold.json`; additive columns are not breaking |
| Support | `ops.pipeline_runs` for status; data-engineering on-call for incidents |
