-- Metric view: governed web-shop measures, so every dashboard and every AI answer
-- computes revenue the same way. Query with MEASURE():
--   SELECT year_month, brand, MEASURE(net_revenue) FROM gold.mv_web_sales GROUP BY 1, 2;
--
-- average_order_value must be a *measure*, not a column: averaging averages is wrong,
-- so it is recomputed at whatever grain the query asks for.
--
-- .optional.sql -> metric views are a preview feature. If this workspace does not
-- support them the runner logs a warning and carries on rather than failing the job.
CREATE OR REPLACE VIEW ${catalog}.${gold}.mv_web_sales WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: ${catalog}.${gold}.fact_sales_order_line
joins:
  - name: customer
    source: ${catalog}.${gold}.dim_customer
    on: source.customer_sk = customer.customer_sk
  - name: product
    source: ${catalog}.${gold}.dim_product
    on: source.product_sk = product.product_sk
  - name: promotion
    source: ${catalog}.${gold}.dim_promotion
    on: source.promo_id = promotion.promo_id
  - name: calendar
    source: ${catalog}.${gold}.dim_date
    on: source.order_date_key = calendar.date_key
dimensions:
  - name: order_date
    expr: source.order_date
  - name: year_month
    expr: calendar.year_month
  - name: year_quarter
    expr: calendar.year_quarter
  - name: brand
    expr: product.brand
  - name: product_name
    expr: product.product_name
  - name: customer_state
    expr: customer.state
  - name: customer_type
    expr: customer.customer_type
  - name: loyalty_segment
    expr: customer.loyalty_segment_name
  - name: promotion_name
    expr: promotion.promotion_name
measures:
  - name: net_revenue
    expr: SUM(source.net_amount)
  - name: gross_revenue
    expr: SUM(source.gross_amount)
  - name: discount_amount
    expr: SUM(source.discount_amount)
  - name: units
    expr: SUM(source.quantity)
  - name: orders
    expr: COUNT(DISTINCT source.order_number)
  - name: order_lines
    expr: COUNT(1)
  - name: customers
    expr: COUNT(DISTINCT source.customer_id)
  - name: average_order_value
    expr: SUM(source.net_amount) / COUNT(DISTINCT source.order_number)
  - name: discount_rate
    expr: SUM(source.discount_amount) / SUM(source.gross_amount)
$$;
