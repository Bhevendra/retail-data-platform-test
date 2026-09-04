# Class 9b — Nested data: when an array deserves its own table

Taught between Class 9 (transformations) and Class 10 (SCD). Students decide, with
evidence, which nested structures in `sales_orders` become Silver tables, then add the
`explode` transformation and build `sales_order_lines` and `sales_order_clicks`.

## Objectives

* State the rule: a nested element with its *own grain* becomes a table; an element that describes its parent row stays an attribute.
* Use `explode` / `posexplode` and read struct fields with dot notation.
* Implement the `explode` step of `apply_transformations` and configure child entities in `silver.json`.
* Reconcile child rows against the parent (sum of line gross = header gross).

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–15 | The pain: three questions that need `LATERAL VIEW` today |
| 15–35 | Mini-lesson: grain and the flatten rule; decide for our three arrays |
| 35–55 | `explode` by hand on `ordered_products`; `posexplode` for line numbers |
| 55–75 | The `explode` step in `apply_transformations`; child entity in config |
| 75–90 | Build `sales_order_lines` and `sales_order_clicks`; reconcile; quality rules per line |
| 90–95 | Homework |

## The pain (15 min)

Ask the class to answer, from `silver.sales_orders` as it stands after Class 9:
"how many units of product X were sold?", "average unit price by brand?", "is any
line's price zero?". Each needs `LATERAL VIEW explode(ordered_products)`. Then ask:
can we write a quality rule `unit_price > 0`? Not against an array. That is the
requirement for today.

## Mini-lesson: the flatten rule (20 min)

Grain again (Class 13 will go deeper): what does one row mean? An order header is
"one order"; an element of `ordered_products` is "one line of one order" — a different
grain, so it deserves its own row and therefore its own table.

Decision table for our arrays (students fill the last column, then compare):

| Array | Element grain | Own table? | Why |
| --- | --- | --- | --- |
| `ordered_products` | one product line of the order | **yes** → `sales_order_lines` | queried, measured, validated per line |
| `clicked_items` | one clicked product | **yes** → `sales_order_clicks` | its own facts (clicks), different products than ordered |
| `promo_info` (order level) | one promotion | **no** | mirrors `promotion_info` on the line (`promo_item` = line product in 428/428 cases) — it is a line *attribute* |

The promotion becomes columns on the line (`promo_id`, `promo_discount_rate`,
`promo_quantity`) and, in Gold, a tiny `dim_promotion` so BI can slice by name.

## Explode by hand (20 min)

```sql
SELECT o.order_number, pos + 1 AS line_number, p.id, p.name, p.qty, p.price, p.promotion_info.promo_disc
FROM retaildataplatform.silver.sales_orders o
LATERAL VIEW posexplode(o.ordered_products) t AS pos, p
WHERE o.order_number = 317568014;
```

PySpark form:

```python
from pyspark.sql import functions as F
lines = (orders.select("*", F.posexplode("ordered_products").alias("__pos", "p"))
               .withColumn("line_number", F.col("__pos") + 1).drop("__pos")
               .withColumn("product_id", F.col("p.id"))
               .withColumn("quantity", F.expr("try_cast(p.qty AS INT)")))
```

Points: one parent row becomes N rows; the struct `p` is addressed with dots; an empty
array produces no rows (`explode_outer` would keep the parent — not wanted here).

## The `explode` step (20 min)

Add to `apply_transformations`, between `parse_json` and `derived` (order matters —
derived expressions need `p.*`):

```python
if t.explode and t.explode.column in df.columns:
    e = t.explode
    if e.position_column:
        df = df.select("*", F.posexplode(F.col(e.column)).alias("__pos", e.alias)).withColumn(e.position_column, F.col("__pos") + 1).drop("__pos")
    else:
        df = df.select("*", F.explode(F.col(e.column)).alias(e.alias))
    if not e.keep_array:
        df = df.drop(e.column)
```

and drop the alias struct at the very end (it is scaffolding). The config model gets an
`Explode` dataclass (`column`, `alias`, `position_column`, `keep_array`) — read it in
`common_utils/config.py`. The child entity in `silver.json`:

```json
{
  "source_table": "sales_orders", "target_table": "sales_order_lines",
  "primary_keys": ["order_number", "line_number"], "scd_type": 1, "order_by": "source_document_id",
  "transformations": {
    "rename": {"_id": "source_document_id"},
    "cast": {"order_number": "bigint", "customer_id": "bigint", "order_datetime": "bigint"},
    "parse_json": {"ordered_products": "array<struct<curr:string,id:string,name:string,price:string,qty:string,unit:string,promotion_info:struct<promo_disc:double,promo_id:string,promo_item:string,promo_qty:string>>>"},
    "explode": {"column": "ordered_products", "alias": "p", "position_column": "line_number"},
    "derived": {"product_id": "p.id", "quantity": "try_cast(p.qty AS INT)", "unit_price": "try_cast(p.price AS DECIMAL(12,2))",
                "promo_id": "coalesce(p.promotion_info.promo_id, 'NONE')", "promo_discount_rate": "coalesce(p.promotion_info.promo_disc, 0)",
                "gross_amount": "CAST(try_cast(p.qty AS INT) * try_cast(p.price AS DECIMAL(12,2)) AS DECIMAL(14,2))"},
    "drop": ["order_datetime", "customer_name", "number_of_line_items", "clicked_items", "promo_info"]
  }
}
```

Three things to discuss: the same Bronze table feeds three Silver entities (header,
lines, clicks); `order_by: source_document_id` with key `(order_number, line_number)`
means the *latest* order version's lines win when an order was re-emitted; SCD1 is
enough because upstream only ever adds lines.

## Build and reconcile (15 min)

Run `b2s` for the three entities. Checks:

```sql
SELECT count(*) FROM retaildataplatform.silver.sales_order_lines;                       -- 7,997
SELECT count(*) FROM retaildataplatform.silver.sales_order_lines GROUP BY order_number, line_number HAVING count(*) > 1;  -- none
SELECT sum(l.gross_amount) = sum(o.order_gross_amount)
FROM retaildataplatform.silver.sales_orders o
JOIN (SELECT order_number, sum(gross_amount) AS gross_amount FROM retaildataplatform.silver.sales_order_lines GROUP BY 1) l USING (order_number)
WHERE o.is_current;                                                                       -- true
```

Then the quality rules that were impossible before: `quantity_positive`,
`unit_price_positive`, `discount_rate_in_range` — all `error`; run and look at
`ops.data_quality_results`.

Gold gets simpler next: `fact_sales_order_line` is now a join of
`silver.sales_order_lines` to the dimensions (no explode), `dim_product` reads distinct
products from a table, and `dim_promotion` is a four-row dimension (`NONE`, 0, 1, 2).

## Homework

1. Add `sales_order_clicks` yourself from the config above (`explode` on `clicked_items`, alias `c`, derived `product_id = c[0]`, `click_count = try_cast(c[1] AS INT)`), key `(order_number, product_id)`.
2. Write the query "products clicked but not ordered, per order" using the two child tables.
3. Argue the opposite case in five lines: when would you keep `ordered_products` as an array in Silver?

## Common problems

* Referencing `p.qty` in `cast` instead of `derived` — `cast` runs *before* explode; struct fields are only available to `derived`.
* Forgetting to drop parent-only columns (`customer_name`, `number_of_line_items`) → they repeat on every line and confuse the hash.
* Exploding *before* dedup: `deduplicate` runs after transformations on `(order_number, line_number)` so both versions of a re-emitted order produce lines — the `order_by` on `source_document_id` is what keeps only the latest; check it is set.
