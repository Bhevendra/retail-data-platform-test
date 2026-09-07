# ADR 0005 – Flatten nested arrays into Silver child tables

**Status**: accepted (2026-09)

## Context
Cosmos DB orders carry `ordered_products`, `clicked_items` and `promo_info` as arrays.
Keeping them as typed arrays on `silver.sales_orders` forced every consumer and every
Gold product to `LATERAL VIEW explode`, prevented element-level quality rules
(`quantity > 0`, `unit_price > 0`) and made Silver unusable for analysts without Spark
array skills.

## Decision
Silver gets one table per grain: `sales_orders` (header, SCD2), `sales_order_lines`
(order x line, SCD1, from the latest order version) and `sales_order_clicks`
(order x clicked product, SCD1). One notebook, `src/silver/code/sales_orders_silver.ipynb`,
builds all three from a single read: it parses the JSON, de-duplicates to one document
per order, derives the header counts and totals, then explodes the arrays. Line-level promotions stay attributes of the line
(`promo_item` always equals the line's product); Gold adds a small `dim_promotion`.

## Consequences
* Gold facts are plain joins; `dim_product` reads a table, not an array.
* Line reconciliation (sum of line gross = header gross) is enforced at Silver.
* The de-duplication happens **before** the explode. Cosmos re-sends a changed order as
  a new document, and an older document can carry lines the newer one dropped; de-
  duplicating only the header would leave those orphan lines and break the line-to-
  header reconciliation.
* Lines removed upstream are not deleted (the source only adds lines); enable delete
  detection or rebuild if that changes.
