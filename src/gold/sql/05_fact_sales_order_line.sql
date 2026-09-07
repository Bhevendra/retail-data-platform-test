-- Web-shop order lines.
-- GRAIN: one row per order_number x line_number, for the current version of each order.
--
-- The measures were computed once in Silver and are reused here, not recalculated —
-- so line arithmetic can never drift between layers.
--
-- LEFT JOIN + coalesce(..., -1): a fact must never be dropped because a dimension row
-- is missing. An INNER join here would silently delete revenue.
CREATE OR REPLACE TABLE ${catalog}.${gold}.fact_sales_order_line AS
SELECT
    concat_ws('-', CAST(l.order_number AS STRING), CAST(l.line_number AS STRING)) AS order_line_id,
    l.order_number,
    l.line_number,
    coalesce(c.customer_sk, -1)                          AS customer_sk,
    coalesce(p.product_sk, -1)                           AS product_sk,
    l.promo_id,
    CAST(date_format(l.order_date, 'yyyyMMdd') AS INT)   AS order_date_key,
    l.order_ts,
    l.order_date,
    l.customer_id,
    l.product_id,
    l.quantity,
    l.unit_price,
    l.currency,
    l.gross_amount,
    l.promo_discount_rate,
    l.discount_amount,
    l.net_amount,
    current_timestamp() AS _updated_at
FROM ${catalog}.${silver}.sales_order_lines l
LEFT JOIN ${catalog}.${gold}.dim_customer c ON c.customer_id = l.customer_id AND c.is_current = true
LEFT JOIN ${catalog}.${gold}.dim_product  p ON p.product_id  = l.product_id;
