-- One big table: web-shop order lines with every dimension already joined.
-- For ad-hoc analysis, notebooks and AI assistants that would rather not write joins.
-- INNER joins are safe here precisely because of the Unknown members (-1 / 'NONE').
CREATE OR REPLACE VIEW ${catalog}.${gold}.sales_order_lines_obt AS
SELECT
    f.order_line_id, f.order_number, f.line_number, f.order_ts, f.order_date,
    d.year, d.year_quarter, d.year_month, d.month_name, d.day_name, d.is_weekend,
    c.customer_id, c.customer_name, c.customer_type, c.state, c.city,
    c.loyalty_segment_name, c.is_active AS customer_is_active,
    p.product_id, p.product_name, p.brand,
    pm.promo_id, pm.promotion_name,
    f.quantity, f.unit_price, f.currency,
    f.gross_amount, f.promo_discount_rate, f.discount_amount, f.net_amount
FROM ${catalog}.${gold}.fact_sales_order_line f
JOIN ${catalog}.${gold}.dim_customer  c  ON c.customer_sk = f.customer_sk
JOIN ${catalog}.${gold}.dim_product   p  ON p.product_sk  = f.product_sk
JOIN ${catalog}.${gold}.dim_promotion pm ON pm.promo_id   = f.promo_id
LEFT JOIN ${catalog}.${gold}.dim_date d  ON d.date_key    = f.order_date_key;
