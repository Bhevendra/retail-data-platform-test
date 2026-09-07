-- Promotion dimension, derived from the promotions actually applied to order lines.
-- 'NONE' is the member for lines without a promotion, which keeps the fact -> dimension
-- join safe as an INNER join in the one-big-table views.
CREATE OR REPLACE TABLE ${catalog}.${gold}.dim_promotion AS
SELECT
    promo_id,
    CASE WHEN promo_id = 'NONE' THEN 'No promotion'
         ELSE concat('Promotion ', promo_id, ' (', CAST(round(max(promo_discount_rate) * 100) AS INT), '% off)')
    END                 AS promotion_name,
    max(promo_discount_rate) AS discount_rate,
    count(*)                 AS lines_applied,
    current_timestamp()      AS _updated_at
FROM ${catalog}.${silver}.sales_order_lines
GROUP BY promo_id;
