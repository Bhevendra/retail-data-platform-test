-- Current version of every real customer. Use this for "as of today" reporting;
-- use dim_customer directly when you need history.
CREATE OR REPLACE VIEW ${catalog}.${gold}.customers_current AS
SELECT * FROM ${catalog}.${gold}.dim_customer
WHERE is_current = true AND customer_sk <> -1;
