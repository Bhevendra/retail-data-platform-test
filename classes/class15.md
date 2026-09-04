# Class 15 — Facts, views and the Gold builder

## Objectives

* Build `fact_sales_order_line` from `silver.sales_order_lines` with surrogate-key lookups (no explode needed since Class 9b).
* Build `fact_sales_order` and `fact_pos_sale`; write reconciliation queries.
* Create the one-big-table views.
* Turn the SQL into config products and build them in dependency order (`s2g` v1).

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | Recap: dims exist; a fact needs FKs to them |
| 10–40 | `fact_sales_order_line` in stages |
| 40–55 | `fact_sales_order`, `fact_pos_sale` |
| 55–65 | Reconciliation queries |
| 65–75 | OBT views |
| 75–95 | `gold.json` products + dependency order → `s2g` v1 |
| 95–100 | Homework |

## `fact_sales_order_line` (30 min)

Because Class 9b flattened the lines, the fact is a projection of
`silver.sales_order_lines` plus key lookups — no explode:

```sql
SELECT concat_ws('-', CAST(l.order_number AS STRING), CAST(l.line_number AS STRING)) AS order_line_id,
       l.order_number, l.line_number,
       coalesce(c.customer_sk, -1) AS customer_sk,
       coalesce(pr.product_sk, -1) AS product_sk,
       l.promo_id,
       CAST(date_format(l.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
       l.order_ts, l.order_date, l.customer_id, l.product_id, l.quantity, l.unit_price, l.currency,
       l.gross_amount, l.promo_discount_rate, l.discount_amount, l.net_amount
FROM retaildataplatform.silver.sales_order_lines l
LEFT JOIN retaildataplatform.gold.dim_customer c ON c.customer_id = l.customer_id AND c.is_current = true
LEFT JOIN retaildataplatform.gold.dim_product pr ON pr.product_id = l.product_id
```

Teaching points: the measures (`gross_amount`, `discount_amount`, `net_amount`) were
computed once in Silver and are *reused*, not recomputed; `LEFT JOIN` + `coalesce(-1)`
keeps revenue for unknown customers (an inner join would silently drop it — count
`customer_sk = -1` afterwards and discuss what to tell the CRM team); `promo_id` is a
foreign key to `dim_promotion` with `NONE` as its Unknown member; `order_date_key`
stays NULL for the 1% of orders without a timestamp — a documented caveat, not a bug.

Show the class the *old* version of this query (with `LATERAL VIEW posexplode`) from
git history and ask which one they would rather maintain.

## Headers and POS (15 min)

`fact_sales_order` aggregates the lines per `order_number` and takes the click-stream
counts already derived on the Silver header (`clicked_item_count`, `click_count` — the
`aggregate(...)` lambda in `silver.json` is "a for loop written inside SQL"). `fact_pos_sale` is a
straight projection of `silver.sales` with the same key lookups. Paste both from
`gold.json` and read them line by line.

## Reconciliation (10 min)

```sql
SELECT (SELECT sum(net_amount)   FROM retaildataplatform.gold.fact_sales_order_line) AS lines_net,
       (SELECT sum(net_amount)   FROM retaildataplatform.gold.fact_sales_order)      AS headers_net,
       (SELECT sum(gross_amount) FROM retaildataplatform.gold.fact_sales_order_line) AS lines_gross,
       (SELECT sum(order_gross_amount) FROM retaildataplatform.silver.sales_orders WHERE is_current) AS silver_gross;
```

The first two must be equal; the last two must be equal. If not, the star is wrong —
this query becomes an automated test in Class 22.

Also check PK uniqueness and FK orphans with `GROUP BY ... HAVING count(*) > 1` and a
`LEFT ANTI JOIN`. Both become tests too.

## One-big-table views (10 min)

```sql
CREATE OR REPLACE VIEW retaildataplatform.gold.sales_order_lines_obt AS
SELECT f.order_line_id, f.order_number, f.order_date, d.year_month, d.day_name, d.is_weekend,
       c.customer_id, c.customer_name, c.customer_type, c.state, c.loyalty_segment_name,
       p.product_id, p.product_name, p.brand,
       f.quantity, f.unit_price, f.gross_amount, f.discount_amount, f.net_amount
FROM retaildataplatform.gold.fact_sales_order_line f
JOIN retaildataplatform.gold.dim_customer c ON c.customer_sk = f.customer_sk
JOIN retaildataplatform.gold.dim_product p ON p.product_sk = f.product_sk
JOIN retaildataplatform.gold.dim_promotion pm ON pm.promo_id = f.promo_id
LEFT JOIN retaildataplatform.gold.dim_date d ON d.date_key = f.order_date_key;
```

Inner joins are safe here *because* of the Unknown members. Views cost nothing to store
and are what notebooks and Genie use.

## Products in config and the builder (20 min)

Each Gold object becomes an entry in `gold.json`: `name`, `type` (`date_dimension`,
`table`, `view`), `sql`, `primary_key`, `foreign_keys`, `depends_on`. The catalog and
schema names in SQL become `${catalog}.${silver}` / `${gold}` placeholders so the same
config deploys anywhere (Class 21).

Ordering problem: facts need dims first, OBT views need facts. v1 solution students can
understand: the config is *listed* in the right order. v2 (reference
`GoldConfig.ordered_products`) walks `depends_on` + foreign keys — a topological sort;
show the function, explain "visit my dependencies before me", do not implement it in
class.

```python
def render_sql(sql, catalog, silver, gold):
    return sql.replace("${catalog}", catalog).replace("${silver}", silver).replace("${gold}", gold)

for product in config["products"]:
    target = f"retaildataplatform.gold.{product['name']}"
    print("building", product["name"], product["type"])
    if product["type"] == "date_dimension":
        date_dimension(product["start_date"], product["end_date"]).write.format("delta").mode("overwrite").saveAsTable(target)
    elif product["type"] == "table":
        spark.sql(render_sql(product["sql"], "retaildataplatform", "silver", "gold")).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
    elif product["type"] == "view":
        spark.sql(f"CREATE OR REPLACE VIEW {target} AS {render_sql(product['sql'], 'retaildataplatform', 'silver', 'gold')}")
```

Run it end to end; rerun the reconciliation query.

## Homework

1. Add `fact_pos_sale` and `pos_sales_obt` to your `gold.json` and rebuild.
2. Write the query "net revenue by brand and loyalty segment per month, web vs store" using the OBT views. Compare with the numbers in the reference `docs/consumers.md`.
3. Read `GoldConfig.ordered_products` in `common_utils/config.py` and trace it on paper for the six products.

## Common problems

* `overwriteSchema` is required when a Gold table's columns change between runs (full rebuild semantics).
* A fact built before its dimension gives `-1` for every row — order matters; that is the whole point of `depends_on`.
* Decimal overflow warnings — use the explicit `DECIMAL(14,2)` casts shown.
