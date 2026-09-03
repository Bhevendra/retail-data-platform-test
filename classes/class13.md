# Class 13 — Dimensional modelling

Mostly theory with a whiteboard and a few queries. Students leave with the Gold design
of the project drawn by themselves.

## Objectives

* Explain facts, dimensions, grain, measures, surrogate keys, conformed dimensions, Unknown members and the date dimension.
* Design the star schema for the retail platform from the Silver tables.
* Explain why BI tools and LLM agents prefer a star schema over wide Silver tables.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–15 | The problem with querying Silver directly |
| 15–45 | Mini-lesson: facts and dimensions (with the analyst's question) |
| 45–65 | Keys: business vs surrogate; Unknown members; date keys |
| 65–85 | Workshop: design our star on the board |
| 85–100 | Grain statements and the reconciliation promise; homework |

## The problem (15 min)

Ask an analyst's question and try to answer it from Silver:
"net revenue by brand and loyalty segment per month, web vs store".

* Brand is inside a JSON array in `sales_orders`; loyalty in `customers`; store sales in `sales`.
* Every analyst would explode, join and filter `is_current` differently → different numbers in different dashboards.
* Power BI cannot auto-detect joins; Genie cannot guess which column is revenue.

Conclusion: Gold's job is to answer the *shape* of questions once, correctly.

## Mini-lesson: facts and dimensions (30 min)

* **Fact** = something that happened, with numbers to add up (measures). Order line: quantity, unit price, net amount.
* **Dimension** = the who / what / when / where you slice by. Customer, product, date.
* **Grain** = what one fact row represents. Say it as a sentence: "one row per order line". Everything else follows from the grain.
* Star schema drawing: fact in the middle, dimensions around it, arrows fact → dimension.
* **Conformed dimension**: the same `dim_customer` is used by web-shop facts and POS facts, so "by loyalty segment" means the same thing in both.
* Slowly changing dimension in Gold: `dim_customer` has one row per *version* (from Silver SCD2); a fact points at the version.

## Keys (20 min)

* Business key (`customer_id`) comes from the source; it can repeat across versions.
* Surrogate key (`customer_sk`) is made by us, unique per version: `xxhash64(customer_id, effective_from)`. Facts store `customer_sk`.
* Date key `yyyymmdd` integer (`20260903`): readable, sortable, small.
* **Unknown member**: a fact whose customer is not in the CRM must not be dropped (revenue would vanish) and must not have a NULL key (joins would lose it). So dimensions get a row `-1 / 'Unknown'` and facts use `coalesce(customer_sk, -1)`. Show the class the count of orders whose customer is missing after the design is built (Class 15).

## Workshop: design our star (20 min)

Groups of three, 12 minutes, then compare with the reference:

| Object | Grain | Keys | Measures / attributes |
| --- | --- | --- | --- |
| `dim_date` | one row per day | `date_key` | year, quarter, month, week, weekend… |
| `dim_customer` | one row per customer version | `customer_sk` (PK), `customer_id` | name, type, geography, loyalty, `is_current` |
| `dim_product` | one row per product | `product_sk` (PK), `product_id` | name, brand, brand_source |
| `fact_sales_order_line` | one row per order × line | `order_line_id` (PK), FKs to the three dims | quantity, unit_price, gross, discount, net |
| `fact_sales_order` | one row per order | `order_number` (PK) | line count, units, amounts, clicks, has_promotion |
| `fact_pos_sale` | one row per product sold in store | `sale_id` (PK) | quantity, unit_price, total_amount |

Discussion points that always come up: "why not one fact for web and store?" (different
grain and no shared id → separate facts, shared dimensions); "where is dim_promotion?"
(promo id and rate live on the line as a degenerate dimension; three values do not
justify a table — but it would be the first thing to add if promotions grew).

## Grain statements and reconciliation (15 min)

Every fact gets a one-sentence grain statement in its `description` in `gold.json` —
show them. Then the reconciliation promise students will *test* in Class 15:
`sum(net_amount)` over lines = `sum(net_amount)` over headers = `sum(order_gross_amount)`
in Silver (minus discounts). If those three differ, the star is wrong.

## Homework

1. Write the grain statement for each of the six Gold objects from memory.
2. For the question "average order value by state and month", list the tables and joins needed in the star.
3. Read `docs/consumers.md` "The model" and note anything that differs from your group's design.

## Common problems (conceptual)

* Confusing "current customer" with "customer at time of order": the fact points at the current version at build time (documented simplification); ask what would be needed for true point-in-time (a version valid at `order_ts`).
* Putting text attributes in facts "because it's easier" — explain the storage and consistency cost.
