# Class 24 — Semantic layer for BI and AI

## Objectives

* Explain what a semantic layer is and why "everyone computes revenue the same way" matters.
* Write a Unity Catalog metric view in YAML (source, joins, dimensions, measures) and query it with `MEASURE()`.
* Connect Power BI (or the SQL editor as a stand-in) to Gold and use PK/FK relationships and `dim_date`.
* Ask Genie / the Assistant questions against Gold and see how comments and constraints shape its SQL.
* Generate the data dictionary and read `docs/consumers.md` as the contract with consumers.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–15 | Three dashboards, three revenue numbers: the problem |
| 15–45 | Metric views: YAML anatomy; `mv_web_sales` built and queried |
| 45–60 | The `metric_view` product type in `gold.py` and why it is best-effort |
| 60–80 | BI: relationships from constraints, date table, current vs historical dims |
| 80–95 | AI: Genie space on `gold`; what comments, FKs and OBT views change; data dictionary |
| 95–100 | Homework |

## Metric views (30 min)

Anatomy on the board, then the real one from `gold.json` (`mv_web_sales.yaml`):

```yaml
version: 0.1
source: ${catalog}.${gold}.fact_sales_order_line
joins:
  - name: customer
    source: ${catalog}.${gold}.dim_customer
    on: source.customer_sk = customer.customer_sk
dimensions:
  - name: year_month
    expr: calendar.year_month
  - name: brand
    expr: product.brand
measures:
  - name: net_revenue
    expr: SUM(source.net_amount)
  - name: orders
    expr: COUNT(DISTINCT source.order_number)
  - name: average_order_value
    expr: SUM(source.net_amount) / COUNT(DISTINCT source.order_number)
```

```sql
CREATE OR REPLACE VIEW retaildataplatform.gold.mv_web_sales WITH METRICS LANGUAGE YAML AS $$ ... $$;
SELECT year_month, brand, MEASURE(net_revenue), MEASURE(orders) FROM retaildataplatform.gold.mv_web_sales GROUP BY 1, 2;
```

Why `average_order_value` must be a measure and not a column: averaging averages is
wrong; the measure is recomputed at whatever grain the query asks for.

## Best-effort product (15 min)

Read `build_product` for `metric_view`: rendered YAML, `CREATE OR REPLACE VIEW … WITH
METRICS`, wrapped in `try/except` that warns unless `strict_metric_views` is `true`.
Discuss: a preview feature must not break the daily load, but in a workspace where it
is supported you want strictness — hence a config flag, not a code change.

## BI (20 min)

In Power BI Desktop (or describe if unavailable): connect via the SQL warehouse to
`retaildataplatform.gold`; relationships appear from PK/FK; mark `dim_date.date` as the
date table; build "net revenue by brand and month". Talk through SCD2 in a report:
`customers_current` for "as of today", the fact's `customer_sk` for "as it was" — the
consumer guide's wording.

## AI (15 min)

Create a Genie space on `gold` and ask: "net revenue by loyalty segment last quarter,
online only". Look at the generated SQL — it uses the joins from the FK constraints and
the column comments to pick `net_amount`. Remove a comment, ask again, compare. Then
show the one-big-table views as the "no joins needed" path for notebooks and agents.

Generate the dictionary: `python -m common_utils.gold` writes `docs/data-dictionary.md`
from the three configs — comments written once are reused by Catalog Explorer, Genie and
the docs.

## Homework

1. Add a measure `discount_rate` (already in the reference) and a dimension `customer_state` to `mv_pos_sales`; rebuild; query it.
2. Write three business questions and, for each, the metric-view query that answers it.
3. Read `docs/consumers.md` "Known data caveats" and add one caveat you discovered yourself.

## Common problems

* `MEASURE()` only works on metric views; on a normal view use `SUM()`.
* Metric view YAML is sensitive to indentation and to `source.` / join-name prefixes.
* If the workspace does not support metric views, `s2g` logs `metric view not created` and continues — that is expected.
