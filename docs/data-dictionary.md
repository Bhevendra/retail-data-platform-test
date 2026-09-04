# Data dictionary

Generated from `src/config/*.json`. Regenerate with `python -m common_utils.gold`.

## Gold (serve here)

### `retaildataplatform.gold.dim_date` (date_dimension)

Calendar dimension, one row per day 2015-2030. Join facts on *_date_key (yyyymmdd integer). Mark `date` as the date column in Power BI.

Primary key: `date_key`

| Column | Description |
| --- | --- |
| `date_key` | Surrogate key in yyyymmdd form, e.g. 20260903. |
| `date` | Calendar date. |
| `year` | Calendar year. |
| `quarter` | Calendar quarter 1-4. |
| `year_quarter` | Year and quarter label, e.g. 2026-Q3. |
| `month` | Month number 1-12. |
| `month_name` | Month name in English. |
| `year_month` | Year and month label yyyy-MM for time-series charts. |
| `iso_week` | ISO-8601 week number. |
| `day_of_month` | Day of month 1-31. |
| `day_of_week` | Day of week 1 (Sunday) - 7 (Saturday). |
| `day_name` | Day name in English. |
| `is_weekend` | TRUE on Saturday and Sunday. |
| `first_day_of_month` | First calendar day of the month. |
| `last_day_of_month` | Last calendar day of the month. |

### `retaildataplatform.gold.dim_customer` (table)

Customer dimension (SCD2). One row per customer version; customer_sk is unique per version, customer_id is the business key. Filter is_current = true for the latest version. Row customer_sk = -1 is the 'Unknown' member facts point to when a customer is missing from the CRM.

Primary key: `customer_sk`

| Column | Description |
| --- | --- |
| `customer_sk` | Surrogate key, unique per customer version. Facts reference this. -1 = Unknown. |
| `customer_id` | Business key from the CRM. |
| `customer_name` | Name as 'LAST, FIRST' for individuals or the organisation name. |
| `customer_type` | 'individual' or 'organisation' (or 'unknown'). |
| `first_name` | Given name(s) for individuals. |
| `last_name` | Family name for individuals. |
| `state` | US state code. |
| `city` | City. |
| `postcode` | Postal code. |
| `street` | Street name. |
| `number` | House / building number. |
| `unit` | Apartment / unit. |
| `region` | Region as captured by the CRM (inconsistent coding). |
| `district` | County / district as captured by the CRM (inconsistent coding). |
| `lon` | Longitude. |
| `lat` | Latitude. |
| `ship_to_address` | Concatenated shipping address. |
| `loyalty_segment` | Loyalty segment code 0-3. |
| `loyalty_segment_name` | Loyalty segment label: Bronze, Silver, Gold, Platinum. |
| `units_purchased` | Lifetime units purchased per the CRM. |
| `is_active` | TRUE when the CRM record is still open. |
| `crm_valid_from` | Validity start of this record in the CRM (UTC). |
| `crm_valid_to` | Validity end in the CRM (UTC); NULL while active. |
| `effective_from` | Version valid from (UTC) in this platform. |
| `effective_to` | Version valid until (UTC); 9999-12-31 for the current version. |
| `is_current` | TRUE for the latest version of the customer. |

### `retaildataplatform.gold.dim_product` (table)

Product dimension built from every product seen in web orders and point-of-sale transactions (there is no product master upstream). Brand comes from the POS export when available, otherwise from the brand name found in the product name. Row product_sk = -1 is the 'Unknown' member.

Primary key: `product_sk`

| Column | Description |
| --- | --- |
| `product_sk` | Surrogate key. Facts reference this. -1 = Unknown. |
| `product_id` | Catalogue product id shared by the web shop and POS. |
| `product_name` | Product name. |
| `brand` | Brand: from the POS export, else matched in the product name against known brands, else the leading word of the product name, else Unknown. See brand_source. |
| `brand_source` | How brand was determined: pos_export, name_match, name_prefix or unknown. |
| `times_sold` | Number of order lines / POS rows the product appeared in (helper for sorting). |

### `retaildataplatform.gold.dim_promotion` (table)

Promotion dimension derived from the promotions seen on order lines (no promotion master upstream). promo_id 'NONE' is the member for lines without a promotion.

Primary key: `promo_id`

| Column | Description |
| --- | --- |
| `promo_id` | Promotion code from the web shop; NONE = no promotion. |
| `promotion_name` | Human-readable label with the discount percentage. |
| `discount_rate` | Discount rate applied by the promotion. |
| `lines_applied` | Order lines the promotion was applied to (helper). |

### `retaildataplatform.gold.fact_sales_order_line` (table)

Web-shop order lines. Grain: one row per order_number x line_number for the current version of each order. Amounts are in `currency` (USD). net_amount applies the line's promotion discount. Source: silver.sales_order_lines.

Primary key: `order_line_id`
Foreign key: `customer_sk` -> `dim_customer(customer_sk)`
Foreign key: `product_sk` -> `dim_product(product_sk)`
Foreign key: `promo_id` -> `dim_promotion(promo_id)`
Foreign key: `order_date_key` -> `dim_date(date_key)`

| Column | Description |
| --- | --- |
| `order_line_id` | order_number-line_number. |
| `order_number` | Web-shop order number (degenerate dimension). |
| `line_number` | 1-based position of the line within the order. |
| `customer_sk` | FK to dim_customer (current version at build time; -1 if unknown). |
| `product_sk` | FK to dim_product (-1 if unknown). |
| `order_date_key` | FK to dim_date; NULL when the source order had no timestamp. |
| `order_ts` | Order timestamp (UTC). |
| `order_date` | Order date (UTC). |
| `customer_id` | Customer business key (for lookups without the dimension). |
| `product_id` | Product business key. |
| `quantity` | Units ordered. |
| `unit_price` | Unit price before discount. |
| `currency` | Currency of the amounts. |
| `gross_amount` | quantity x unit_price. |
| `promo_id` | FK to dim_promotion; NONE when no promotion. |
| `promo_discount_rate` | Discount rate applied to the line (0 when none). |
| `discount_amount` | gross_amount x promo_discount_rate. |
| `net_amount` | gross_amount - discount_amount. Use this for revenue. |

### `retaildataplatform.gold.fact_sales_order` (table)

Web-shop order headers. Grain: one row per order_number (current version). Aggregates of fact_sales_order_line plus click-stream and promotion flags.

Primary key: `order_number`
Foreign key: `customer_sk` -> `dim_customer(customer_sk)`
Foreign key: `order_date_key` -> `dim_date(date_key)`

| Column | Description |
| --- | --- |
| `order_number` | Web-shop order number. |
| `customer_sk` | FK to dim_customer (-1 if unknown). |
| `order_date_key` | FK to dim_date; NULL when the order had no timestamp. |
| `order_ts` | Order timestamp (UTC). |
| `order_date` | Order date (UTC). |
| `customer_id` | Customer business key. |
| `line_item_count` | Number of lines in the order. |
| `units` | Total units across lines. |
| `gross_amount` | Sum of line gross amounts. |
| `discount_amount` | Sum of line discounts. |
| `net_amount` | Sum of line net amounts. Use this for revenue. |
| `has_promotion` | TRUE when a promotion was applied. |
| `clicked_item_count` | Distinct products clicked before checkout. |
| `click_count` | Total clicks recorded before checkout. |
| `version_effective_from` | When the current version of the order was loaded (UTC). |

### `retaildataplatform.gold.fact_pos_sale` (table)

Point-of-sale transactions from the store export. Grain: one row per product sold (sale_id). Not linked to web-shop orders.

Primary key: `sale_id`
Foreign key: `customer_sk` -> `dim_customer(customer_sk)`
Foreign key: `product_sk` -> `dim_product(product_sk)`
Foreign key: `sale_date_key` -> `dim_date(date_key)`

| Column | Description |
| --- | --- |
| `sale_id` | Deterministic id of the POS row. |
| `customer_sk` | FK to dim_customer (-1 if unknown). |
| `product_sk` | FK to dim_product (-1 if unknown). |
| `sale_date_key` | FK to dim_date. |
| `sale_date` | Sale date. |
| `customer_id` | Customer business key. |
| `product_id` | Product business key. |
| `quantity` | Units sold. |
| `unit_price` | Unit price. |
| `currency` | Currency of the amounts. |
| `total_amount` | Sale amount (quantity x unit_price). Use this for POS revenue. |

### `retaildataplatform.gold.customers_current` (view)

Current version of every customer (excludes the Unknown member).


### `retaildataplatform.gold.sales_order_lines_obt` (view)

One-big-table view: web-shop order lines joined to customer, product and date. Best starting point for ad-hoc analysis, notebooks and Genie.


| Column | Description |
| --- | --- |
| `net_amount` | Revenue for the line after discount. |
| `customer_name` | Customer name (PII). |
| `brand` | Product brand. |

### `retaildataplatform.gold.pos_sales_obt` (view)

One-big-table view: POS sales joined to customer, product and date.


### `retaildataplatform.gold.mv_web_sales` (metric_view)

Governed web-shop sales metrics (revenue, orders, units, discount) by date, customer and product attributes. Use in dashboards and Genie instead of hand-written aggregations.


### `retaildataplatform.gold.mv_pos_sales` (metric_view)

Governed point-of-sale metrics (revenue, units, transactions) by date, customer and product attributes.


## Silver (conformed)

### `retaildataplatform.silver.sales_orders` (SCD type 2, key `order_number`)

Web-shop order headers (Cosmos DB), one row per order version (SCD2). The source re-emits an order when line items are added, so the same order_number can appear more than once in an extract; the latest document (by Cosmos _id) wins and earlier states are kept as history. Line items and click-stream live in silver.sales_order_lines and silver.sales_order_clicks; promotions are attributes of the line.

| Column | Description |
| --- | --- |
| `order_number` | Business key of the order (web shop order number). |
| `source_document_id` | Cosmos DB document _id; increases with insertion time and orders versions of the same order. |
| `customer_id` | Customer business key; joins silver.customers.customer_id. |
| `customer_name` | Customer name as captured at order time (may differ from the CRM master). |
| `number_of_line_items` | Line-item count reported by the source (can lag ordered_products; see line_item_count). |
| `line_item_count` | Actual number of entries in ordered_products. |
| `order_ts` | Order timestamp (UTC), derived from the source epoch seconds. NULL for a small share of orders. |
| `order_date` | Order date (UTC). |
| `has_promotion` | TRUE when at least one promotion was applied. |
| `order_gross_amount` | Sum of price x quantity over ordered_products, before discounts. |
| `click_count` | Total product clicks recorded before checkout. |
| `clicked_item_count` | Distinct products clicked before checkout. |

### `retaildataplatform.silver.sales_order_lines` (SCD type 1, key `order_number, line_number`)

Web-shop order lines, one row per order_number x line_number for the latest version of each order (SCD1: lines are only ever added upstream). Flattened from the ordered_products array; the promotion applied to the line is carried as attributes (promo_item always equals the line's product).

| Column | Description |
| --- | --- |
| `order_number` | Order business key; joins silver.sales_orders. |
| `line_number` | 1-based position of the line within the order. |
| `source_document_id` | Cosmos document the line was taken from (latest version of the order). |
| `customer_id` | Customer business key. |
| `order_ts` | Order timestamp (UTC). |
| `order_date` | Order date (UTC). |
| `product_id` | Catalogue product id. |
| `product_name` | Product name as ordered. |
| `quantity` | Units ordered. |
| `unit_price` | Unit price before discount. |
| `currency` | Currency of the amounts. |
| `unit` | Unit of measure (pcs). |
| `promo_id` | Promotion applied to the line; NONE when no promotion. Joins gold.dim_promotion. |
| `promo_discount_rate` | Discount rate applied to the line (0 when none). |
| `promo_quantity` | Quantity the promotion applied to, per the source. |
| `gross_amount` | quantity x unit_price. |
| `discount_amount` | gross_amount x promo_discount_rate. |
| `net_amount` | gross_amount - discount_amount. Use this for revenue. |

### `retaildataplatform.silver.sales_order_clicks` (SCD type 1, key `order_number, product_id`)

Products a customer clicked before checking out, one row per order_number x product_id, from the clicked_items click-stream (SCD1).

| Column | Description |
| --- | --- |
| `order_number` | Order business key. |
| `product_id` | Clicked product id (may not have been ordered). |
| `click_count` | Number of clicks on the product before checkout. |
| `click_rank` | Position in the click list. |
| `customer_id` | Customer business key. |
| `source_document_id` | Cosmos document the row was taken from. |

### `retaildataplatform.silver.sales` (SCD type 1, key `sale_id`)

Point-of-sale transactions exported to S3, one row per product sold (SCD1). The feed has no transaction identifier, so sale_id is a deterministic hash of the business columns; exact duplicate rows in the export collapse into one. The nested product JSON is parsed with regular expressions because ~8% of names contain unescaped quotes.

| Column | Description |
| --- | --- |
| `sale_id` | Deterministic SHA-256 over customer, date, product payload and amount; stable across reloads. |
| `customer_id` | Customer business key; joins silver.customers.customer_id. |
| `customer_name` | Customer name as printed on the receipt. |
| `listed_product_name` | Product name column from the export; often differs from the product actually sold (see product_name). |
| `product_id` | Catalogue product id of the item sold (parsed from the product payload). |
| `product_name` | Name of the item sold (parsed from the product payload; authoritative). |
| `brand` | Brand/category reported by the export. |
| `order_date` | Sale date. |
| `unit_price` | Unit price in `currency`. |
| `quantity` | Units sold. |
| `currency` | ISO currency code of the amounts. |
| `total_amount` | Line total from the export (= unit_price x quantity). |

### `retaildataplatform.silver.customers` (SCD type 2, key `customer_id`)

Customer master from the CRM (Azure SQL). The source carries its own validity window (valid_from / valid_to) and can contain more than one version per customer; the latest source version per customer_id is loaded and change history across loads is kept (SCD2). Use is_current = true for the latest version and is_active for customers still open in the CRM.

| Column | Description |
| --- | --- |
| `customer_id` | Customer business key (CRM id). |
| `tax_id` | Tax identifier when known (mostly null). |
| `tax_code` | Tax code when known. |
| `customer_name` | Name as 'LAST, FIRST' for individuals or the organisation name. |
| `customer_type` | 'individual' (name has a comma) or 'organisation'. |
| `last_name` | Family name for individuals; NULL for organisations. |
| `first_name` | Given name(s) for individuals; NULL for organisations. |
| `state` | US state code of the address. |
| `city` | City of the address (may be missing). |
| `postcode` | Postal code, normalised (no trailing .0; 0 becomes NULL). |
| `street` | Street name. |
| `number` | House / building number, normalised. |
| `unit` | Apartment / unit when present. |
| `region` | Region as captured by the CRM (inconsistent: state code or name). |
| `district` | County / district as captured by the CRM (inconsistent codes and names). |
| `lon` | Longitude of the address. |
| `lat` | Latitude of the address. |
| `ship_to_address` | Concatenated shipping address string from the CRM. |
| `source_valid_from` | CRM validity start (epoch seconds). |
| `source_valid_to` | CRM validity end (epoch seconds); NULL while active. |
| `source_valid_from_ts` | CRM validity start (UTC timestamp). |
| `source_valid_to_ts` | CRM validity end (UTC timestamp); NULL while active. |
| `is_active` | TRUE when the CRM record is still open (no valid_to). |
| `units_purchased` | Lifetime units purchased according to the CRM. |
| `loyalty_segment` | Loyalty segment code 0-3 from the CRM. |
| `loyalty_segment_name` | Loyalty segment label (0 Bronze, 1 Silver, 2 Gold, 3 Platinum). |

## Bronze (raw, audit only)

- `retaildataplatform.bronze.sales_orders` <- cosmos_mongodb `cosmos_sales_orders`; PII columns: customer_name
- `retaildataplatform.bronze.sales` <- s3 `s3_sales`; PII columns: customer_name
- `retaildataplatform.bronze.customers` <- sqlserver `sqlserver_customers`; PII columns: tax_id, customer_name, street, number, unit, ship_to_address, lon, lat
