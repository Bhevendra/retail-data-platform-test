-- One big table: store sales with every dimension already joined.
CREATE OR REPLACE VIEW ${catalog}.${gold}.pos_sales_obt AS
SELECT
    f.sale_id, f.sale_date,
    d.year, d.year_quarter, d.year_month, d.month_name, d.day_name, d.is_weekend,
    c.customer_id, c.customer_name, c.customer_type, c.state, c.city, c.loyalty_segment_name,
    p.product_id, p.product_name, p.brand,
    f.quantity, f.unit_price, f.currency, f.total_amount
FROM ${catalog}.${gold}.fact_pos_sale f
JOIN ${catalog}.${gold}.dim_customer c ON c.customer_sk = f.customer_sk
JOIN ${catalog}.${gold}.dim_product  p ON p.product_sk  = f.product_sk
LEFT JOIN ${catalog}.${gold}.dim_date d ON d.date_key   = f.sale_date_key;
