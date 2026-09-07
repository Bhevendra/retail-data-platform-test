-- Point-of-sale transactions.
-- GRAIN: one row per product sold in a store (sale_id).
-- Deliberately a separate fact from the web-shop ones: different grain, no shared
-- transaction id. Compare the channels through the shared dimensions, never row to row.
CREATE OR REPLACE TABLE ${catalog}.${gold}.fact_pos_sale AS
SELECT
    s.sale_id,
    coalesce(c.customer_sk, -1)                         AS customer_sk,
    coalesce(p.product_sk, -1)                          AS product_sk,
    CAST(date_format(s.order_date, 'yyyyMMdd') AS INT)  AS sale_date_key,
    s.order_date AS sale_date,
    s.customer_id,
    s.product_id,
    s.quantity,
    s.unit_price,
    s.currency,
    s.total_amount,
    current_timestamp() AS _updated_at
FROM ${catalog}.${silver}.sales s
LEFT JOIN ${catalog}.${gold}.dim_customer c ON c.customer_id = s.customer_id AND c.is_current = true
LEFT JOIN ${catalog}.${gold}.dim_product  p ON p.product_id  = s.product_id;
