-- Metric view: governed store measures, mirroring mv_web_sales so the two channels
-- can be compared on identical definitions.
CREATE OR REPLACE VIEW ${catalog}.${gold}.mv_pos_sales WITH METRICS LANGUAGE YAML AS $$
version: 0.1
source: ${catalog}.${gold}.fact_pos_sale
joins:
  - name: customer
    source: ${catalog}.${gold}.dim_customer
    on: source.customer_sk = customer.customer_sk
  - name: product
    source: ${catalog}.${gold}.dim_product
    on: source.product_sk = product.product_sk
  - name: calendar
    source: ${catalog}.${gold}.dim_date
    on: source.sale_date_key = calendar.date_key
dimensions:
  - name: sale_date
    expr: source.sale_date
  - name: year_month
    expr: calendar.year_month
  - name: brand
    expr: product.brand
  - name: product_name
    expr: product.product_name
  - name: customer_state
    expr: customer.state
  - name: loyalty_segment
    expr: customer.loyalty_segment_name
measures:
  - name: revenue
    expr: SUM(source.total_amount)
  - name: units
    expr: SUM(source.quantity)
  - name: transactions
    expr: COUNT(1)
  - name: customers
    expr: COUNT(DISTINCT source.customer_id)
  - name: average_transaction_value
    expr: SUM(source.total_amount) / COUNT(1)
$$;
