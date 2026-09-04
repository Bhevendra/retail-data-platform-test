# Class 8 — Data profiling workshop

Before writing a single Silver transformation, students discover every quirk of the
real data themselves, using only skills they already have. The findings list they
produce is the specification for Classes 9–12.

## Objectives

* Profile a Bronze table systematically: row counts, key uniqueness, nulls, value ranges, formats.
* Recognise the six classic problems: duplicate keys, versioned rows, NULL literals, epoch timestamps, nested JSON, malformed JSON.
* Write a findings list with a decision for each finding.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | The profiling checklist (board) |
| 10–35 | Profile `customers` together |
| 35–60 | Profile `sales_orders` in pairs (instructor circulates) |
| 60–80 | Profile `sales` in pairs |
| 80–95 | Consolidate the findings list; decisions |
| 95–100 | Homework |

## The checklist (10 min)

For every table ask, in this order:

1. How many rows? How many distinct values of the column I think is the key?
2. Which columns have NULLs, and are there *fake* NULLs (`'NULL'`, `''`, `'nan'`, `'N/A'`)?
3. What is the type of each column versus what it *should* be?
4. Are there columns holding structured text (JSON, comma lists)?
5. Are there rows that are exact duplicates?
6. Do numbers reconcile (e.g. total = price × qty)?

## `customers` together (25 min)

```python
c = spark.table("retaildataplatform.bronze.customers")
print(c.count(), c.select("customer_id").distinct().count())
```

28,813 rows but 27,523 ids → **finding 1: customer_id is not unique**. Dig:

```python
from pyspark.sql import functions as F
dup = c.groupBy("customer_id").count().filter("count > 1")
print(dup.count())
display(c.join(dup, "customer_id").orderBy("customer_id", "valid_from").select("customer_id", "customer_name", "valid_from", "valid_to", "units_purchased").limit(10))
```

Same customer, two rows, one with `valid_to` filled and one with NULL → the CRM keeps
its own history. **Decision: keep the latest version per customer (max `valid_from`).**

```python
display(c.select([F.sum(F.col(x).isNull().cast("int")).alias(x) for x in c.columns]))
display(c.filter("postcode = 'NULL' OR unit = 'NULL' OR city = 'NULL'").limit(5))
```

**Finding 2: the text `'NULL'` is not a NULL.** Decision: treat `'NULL'`, `''`, `'nan'`, `'NA'`, `'NA NA'` as NULL in Silver.

```python
display(c.select("postcode", "number", "district", "valid_from").limit(10))
```

**Finding 3:** `postcode` `46506.0`, `number` `521.0` — numbers formatted as floats then
stored as text. Decision: strip trailing `.0`. **Finding 4:** `valid_from` is epoch
seconds; `F.from_unixtime` / `CAST(x AS TIMESTAMP)` converts it. **Finding 5:**
`customer_name` is `LAST,  FIRST` (two spaces) for people, plain text for companies.
Decision: derive `customer_type`, `first_name`, `last_name`, collapse spaces.

## `sales_orders` in pairs (25 min)

Prompts on the board; pairs report back:

* Is `order_number` unique? (No: 74 duplicated. Compare the two rows — `number_of_line_items` differs → the source re-emits an order when lines are added. **Decision: latest document wins; keep old versions as history.**)
* Type of `order_datetime`? (double/long epoch; 1% NULL. Decision: convert, keep NULLs, warn.)
* What is inside `ordered_products`? (JSON string of a list of dicts; `price` and `qty` are *strings*. Decision: parse with an explicit schema.)
* Does `number_of_line_items` equal the real number of products? (Not always. Decision: keep both, warn.)
* `promo_info` — what values? (`[]` or a list; discount rate 3/5/7 %.)

## `sales` in pairs (20 min)

* Is there any key column? (No. **Decision: build a hash of the whole row as `sale_id`.**)
* Exact duplicate rows? (`s.count() - s.distinct().count()` → 8. Decision: collapse.)
* Does `product` JSON parse? Try `F.from_json` with a struct schema and count NULLs → 30 rows fail because the product name contains an unescaped `"`. **Decision: parse with `regexp_extract` instead of `from_json`.**
* `total_price` vs price × qty? (Always equal — good.)
* `product_name` column vs name inside JSON? (Differ in ~70% of rows. Decision: JSON is authoritative; keep the column as `listed_product_name`.)

## Findings list (15 min)

Collect on one slide; this is the contract for Silver:

| # | Table | Finding | Decision |
| --- | --- | --- | --- |
| 1 | customers | duplicate ids = CRM versions | latest `valid_from` wins, history via SCD2 |
| 2 | all | text NULLs | normalise to real NULL |
| 3 | customers | `46506.0` style numbers | strip `.0` |
| 4 | customers/orders | epoch seconds | cast to timestamp |
| 5 | customers | `LAST, FIRST` names | derive type/first/last |
| 6 | orders | duplicate order_number = versions | latest `_id` wins, SCD2 |
| 7 | orders | nested JSON, string numbers | `from_json` with explicit schema; lines and clicks become their own Silver tables (Class 9b) |
| 8 | sales | no key | `sha2` of row as `sale_id` |
| 9 | sales | broken JSON | `regexp_extract` |
| 10 | sales | exact duplicates | dedupe via key hash |

Show that every line of `src/config/silver.json` corresponds to a row of this table.

## Homework

1. For findings 3 and 5 write the PySpark expression (`regexp_replace`, `split_part`, `CASE WHEN`) that fixes them, and test on 5 rows.
2. Parse `ordered_products` with `F.from_json` and a schema string you write yourself; `explode` it and count lines.
3. Compute `sha2(concat_ws('|', ...), 256)` over the `sales` columns and check that the 8 duplicates now share a hash.

## Common problems

* `from_json` returning all NULLs — schema string mismatch (case, missing field). Print one raw string and build the schema field by field.
* Students "fix" data by filtering rows away — remind them: Silver keeps rows and marks problems; only quality rules (Class 18) decide what is blocked.
