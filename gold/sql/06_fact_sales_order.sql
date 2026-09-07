-- Web-shop order headers.
-- GRAIN: one row per order_number (current version).
-- Amounts are aggregated from the line fact so header and line totals agree by
-- construction; click counts come from the Silver header.
CREATE OR REPLACE TABLE ${catalog}.${gold}.fact_sales_order AS
SELECT
    o.order_number,
    coalesce(c.customer_sk, -1)                          AS customer_sk,
    CAST(date_format(o.order_date, 'yyyyMMdd') AS INT)   AS order_date_key,
    o.order_ts,
    o.order_date,
    o.customer_id,
    o.line_item_count,
    coalesce(l.units, 0)             AS units,
    coalesce(l.gross_amount, 0)      AS gross_amount,
    coalesce(l.discount_amount, 0)   AS discount_amount,
    coalesce(l.net_amount, 0)        AS net_amount,
    o.has_promotion,
    o.clicked_item_count,
    o.click_count,
    o.effective_from AS version_effective_from,
    current_timestamp() AS _updated_at
FROM ${catalog}.${silver}.sales_orders o
LEFT JOIN (
    SELECT order_number,
           sum(quantity)        AS units,
           sum(gross_amount)    AS gross_amount,
           sum(discount_amount) AS discount_amount,
           sum(net_amount)      AS net_amount
    FROM ${catalog}.${gold}.fact_sales_order_line
    GROUP BY order_number
) l ON l.order_number = o.order_number
LEFT JOIN ${catalog}.${gold}.dim_customer c ON c.customer_id = o.customer_id AND c.is_current = true
WHERE o.is_current = true;
