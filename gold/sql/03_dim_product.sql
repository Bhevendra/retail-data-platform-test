-- Product dimension. There is no product master upstream, so it is derived from every
-- product ever seen in a web order or a store sale.
--
-- Brand is a best-effort chain, and `brand_source` records which step produced it so
-- analysts can judge how much to trust it:
--   1. the brand column on the POS export        -> 'pos_export'
--   2. a known brand name found in the product   -> 'name_match'
--   3. the first word of the product name        -> 'name_prefix'
--   4. nothing worked                            -> 'Unknown'
CREATE OR REPLACE TABLE ${catalog}.${gold}.dim_product AS
WITH order_products AS (
    SELECT product_id, product_name FROM ${catalog}.${silver}.sales_order_lines
),
pos_products AS (
    SELECT product_id, product_name, brand
    FROM ${catalog}.${silver}.sales
    WHERE product_id IS NOT NULL AND product_id <> ''
),
known_brands AS (
    SELECT DISTINCT brand FROM pos_products WHERE brand IS NOT NULL
),
all_products AS (
    SELECT product_id, max(product_name) AS product_name, count(*) AS times_sold
    FROM (SELECT product_id, product_name FROM order_products
          UNION ALL
          SELECT product_id, product_name FROM pos_products)
    GROUP BY product_id
)
SELECT
    xxhash64(p.product_id) AS product_sk,
    p.product_id,
    p.product_name,
    coalesce(max(pos.brand),
             max(b.brand),
             nullif(regexp_extract(p.product_name, '^([A-Za-z]{3,})', 1), ''),
             'Unknown')                                                     AS brand,
    CASE WHEN max(pos.brand) IS NOT NULL                                    THEN 'pos_export'
         WHEN max(b.brand)   IS NOT NULL                                    THEN 'name_match'
         WHEN regexp_extract(p.product_name, '^([A-Za-z]{3,})', 1) <> ''    THEN 'name_prefix'
         ELSE 'unknown' END                                                 AS brand_source,
    max(p.times_sold)   AS times_sold,
    current_timestamp() AS _updated_at
FROM all_products p
LEFT JOIN pos_products pos ON pos.product_id = p.product_id
LEFT JOIN known_brands b   ON p.product_name ILIKE concat('%', b.brand, '%')
GROUP BY p.product_id, p.product_name
UNION ALL
SELECT -1, NULL, 'Unknown', 'Unknown', 'unknown', 0, current_timestamp();
