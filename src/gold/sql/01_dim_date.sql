-- Calendar dimension, one row per day. Facts join on *_date_key (yyyymmdd integer).
-- Range is fixed rather than "min to max order date" so keys stay stable and BI can
-- plan into the future. Mark `date` as the date table in Power BI.
CREATE OR REPLACE TABLE ${catalog}.${gold}.dim_date AS
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INT)          AS date_key,
    date,
    year(date)                                          AS year,
    quarter(date)                                       AS quarter,
    concat(year(date), '-Q', quarter(date))             AS year_quarter,
    month(date)                                         AS month,
    date_format(date, 'MMMM')                           AS month_name,
    date_format(date, 'yyyy-MM')                        AS year_month,
    weekofyear(date)                                    AS iso_week,
    dayofmonth(date)                                    AS day_of_month,
    dayofweek(date)                                     AS day_of_week,
    date_format(date, 'EEEE')                           AS day_name,
    dayofweek(date) IN (1, 7)                           AS is_weekend,
    CAST(date_trunc('month', date) AS DATE)             AS first_day_of_month,
    last_day(date)                                      AS last_day_of_month,
    current_timestamp()                                 AS _updated_at
FROM (SELECT explode(sequence(DATE'2015-01-01', DATE'2030-12-31', INTERVAL 1 DAY)) AS date);
