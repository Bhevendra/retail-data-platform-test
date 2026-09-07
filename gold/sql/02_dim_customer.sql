-- Customer dimension, one row per customer *version* (Silver keeps SCD2 history).
--   customer_sk  unique per version — this is what facts point at
--   customer_id  the CRM business key, repeats across versions
--   is_current   filter on this for "as of today" reporting
-- The UNION ALL row is the Unknown member: a fact whose customer is missing from the
-- CRM points at -1 rather than being dropped (losing revenue) or NULL (losing joins).
CREATE OR REPLACE TABLE ${catalog}.${gold}.dim_customer AS
SELECT
    xxhash64(CAST(customer_id AS STRING), CAST(effective_from AS STRING)) AS customer_sk,
    customer_id, customer_name, customer_type, first_name, last_name,
    state, city, postcode, street, number, unit, region, district, lon, lat, ship_to_address,
    loyalty_segment, loyalty_segment_name, units_purchased, is_active,
    source_valid_from_ts AS crm_valid_from,
    source_valid_to_ts   AS crm_valid_to,
    effective_from, effective_to, is_current,
    current_timestamp() AS _updated_at
FROM ${catalog}.${silver}.customers
UNION ALL
SELECT -1, NULL, 'Unknown', 'unknown', NULL, NULL,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL, 'Unknown', NULL, NULL,
       NULL, NULL,
       TIMESTAMP'1900-01-01', TIMESTAMP'9999-12-31', true,
       current_timestamp();
