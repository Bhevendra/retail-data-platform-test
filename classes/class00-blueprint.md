# Class 0 — The Data Product Blueprint (filled in for the Retail Data Platform)

Teach before Class 1. Two artefacts: the blank framework
(`classes/templates/DATA-PRODUCT-BLUEPRINT.md`) and this file, the same framework filled
in for our project. Walk the template first (why each section exists, what its gate
means), then fill this one in *with* the class section by section.

Message to repeat: **you rarely have everything at the start.** Each section below
shows its status on "day 1" and what it became. The 🟥 sections are the minimum to
start — above all §5 (source system details): without knowing where the data is, how
to reach it and what its keys and change semantics are, nothing can be implemented.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–20 | The template: 23 sections, three gates, the rules |
| 20–35 | Business scenario and stakeholders (§1–2) — the trigger email and the role-play |
| 35–60 | Source system details (§5), access (§6), classification (§7): what we knew on day 1 |
| 60–80 | Business questions, definitions, refresh, quality (§3, §4, §8, §9): what we agreed during discovery |
| 80–95 | Models and target design (§10–12); a glance at the 🟩 sections we will fill during the build |
| 95–100 | Homework |

---

## 1. Business scenario 🟥

Owner: Head of Retail Analytics · Status: agreed (day 1)

A retailer sells through a web shop and physical stores. Finance's monthly deck,
the e-commerce dashboard and the store POS spreadsheet each report a different revenue
figure; three copies of "customer" disagree on loyalty membership. The trigger was the
sponsor's email ("Numbers don't match again … leadership wants one number by brand,
channel and loyalty segment every morning, and to ask questions in plain English in the
AI assistant, before the holiday season").

Success: *"One trusted, daily, explainable set of sales numbers that Finance, BI and the
AI assistant all agree on."*

Out of scope for v1: streaming, returns/refunds, inventory, a product master (none
exists), currency conversion (all USD), row-level security by region.

## 2. Stakeholders and ownership 🟥

Status: agreed (day 1, names anonymised for the bootcamp)

| Role | R/A/C/I | Decides on |
| --- | --- | --- |
| Head of Retail Analytics (sponsor) | A | scope, priority, sign-off |
| Analytics lead | R | KPI definitions, acceptance questions |
| Data engineering (us) | R | design, build, operations |
| E-commerce platform owner (Cosmos DB) | C | access, schema, extract window |
| Store operations (S3 export) | C | file format, drop time, bucket access |
| CRM administrator (Azure SQL) | C | access, firewall, PII columns |
| Security / compliance | C | secrets, PII handling, retention |
| Finance | C | reconciliation and sign-off of numbers |
| BI analyst, AI/ML engineer | C/I | consumption needs |

Open questions go to the analytics lead; unresolved after five working days → sponsor.

## 3. Business questions and KPIs 🟧

Status: draft on day 1 (only Q1), agreed after discovery week

| # | Question | KPI | Grain | Dimensions | Consumer | Priority |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | Net revenue by month, brand, channel | net_revenue | order line / POS row | month, brand, channel | Finance, BI | must |
| Q2 | Orders, units, AOV by loyalty segment and state | orders, units, average_order_value | order | segment, state, month | BI | must |
| Q3 | Discount rate by promotion and brand | discount_rate | order line | promo_id, brand | Marketing | should |
| Q4 | Customers buying both online and in store | customer count | customer | — | Marketing | should |
| Q5 | Top products by units and revenue per month | units, revenue | product | month | BI | should |
| Q6 | What did customer X buy in September, with attributes then and now | — | order line | customer version | Analysts | could |

KPI formulas: `net_revenue = Σ quantity × unit_price × (1 − promo_discount_rate)`;
`gross_revenue = Σ quantity × unit_price`; `discount_rate = discount / gross`;
`average_order_value = net_revenue / distinct orders`. Signed off by Finance.
Each question must be answerable in Power BI without manual joins, in ≤10 lines of SQL,
and by Genie from natural language.

## 4. Business definitions 🟧

Status: agreed after discovery; two changed during profiling (see §22)

| Term | Definition | Source of truth | Ambiguity resolved |
| --- | --- | --- | --- |
| Customer | CRM record identified by `customer_id`; individual (name `LAST, FIRST`) or organisation | Azure SQL `retail.customers` | CRM keeps versions → "the customer" = latest version; history kept |
| Order | Web-shop order identified by `order_number` | Cosmos `sales_orders` | Re-emitted document = **new version of the same order**, not a new order |
| Order line | One product within an order | `ordered_products` array | Line number = position in the array |
| POS sale | One product sold in a store | S3 `sales.parquet` | No id upstream → deterministic hash of the row; identical rows = duplicates |
| Channel | `web` (orders) vs `store` (POS) | — | Never joined row to row |
| Net revenue | Gross minus the line's promotion discount, before tax | — | Discount applies to the line whose `promo_item` matches |
| Brand | POS category; else brand word in product name; else leading word; else Unknown | derived | Recorded in `brand_source` |
| Active customer | CRM `valid_to` is NULL | CRM | |
| Loyalty segment | CRM code 0–3 → Bronze/Silver/Gold/Platinum | CRM | labels agreed with marketing |

## 5. Source system details 🟥

Status: **known on day 1 for all three — the reason we could start**; "known issues" added after profiling (Class 8)

### 5.1 Web orders — Cosmos DB

| Item | Value |
| --- | --- |
| System | Azure Cosmos DB, Mongo API |
| Object | database `retail`, collection `sales_orders` |
| Owner | e-commerce platform team |
| Access | `pymongo` (Serverless cannot use the Spark Mongo connector) |
| Credentials | secret scope `retail-platform-<env>`, key `cosmos-connection-string` |
| Network | public endpoint, no allow-list needed in dev |
| Extract type | full read of the collection daily |
| Volume | ~4,000 documents, growing |
| Business key | `order_number` — **not unique**: re-emitted when lines are added; `_id` orders versions |
| Change semantics | documents re-emitted with more lines; no deletes |
| Schema | `order_number`, `customer_id`, `customer_name`, `order_datetime` (epoch, 1% null), `number_of_line_items`, `ordered_products` (array of {id, name, price:string, qty:string, curr, unit, promotion_info}), `clicked_items` (array of [id, count]), `promo_info` |
| Known issues | string numbers inside JSON; `number_of_line_items` lags the array in 4% of docs |

### 5.2 POS sales — Amazon S3

| Item | Value |
| --- | --- |
| System | S3 bucket `bucket-rivadata`, region `eu-north-1` |
| Object | `retail/sales.parquet` |
| Owner | store operations |
| Access | `boto3` with access keys (v1); storage credential / IAM role (v2) |
| Credentials | keys `aws-access-key-id`, `aws-secret-access-key` |
| Extract type | nightly file drop, full |
| Volume | ~360 rows |
| Business key | **none** → `sale_id = sha2(customer, date, product payload, listed name, amount)` |
| Change semantics | file replaced nightly; 8 exact duplicate rows |
| Schema | `customer_id`, `customer_name`, `product_name`, `order_date`, `product_category` (brand), `product` (JSON string), `total_price` |
| Known issues | ~8% of product JSON has unescaped quotes → regex parsing; `product_name` column ≠ product inside JSON in ~70% of rows (JSON authoritative) |

### 5.3 Customers — Azure SQL

| Item | Value |
| --- | --- |
| System | Azure SQL Server `rivadata.database.windows.net`, database `batch2` |
| Object | `retail.customers` |
| Owner | CRM administrator |
| Access | JDBC (bundled driver); `encrypt=true;trustServerCertificate=true;loginTimeout=90` required from Serverless |
| Credentials | keys `sqlserver-username`, `sqlserver-password` |
| Network | server firewall must allow Databricks Serverless egress |
| Extract type | daily full extract |
| Volume | 28,813 rows / 27,523 customers |
| Business key | `customer_id` + `valid_from` (CRM versions) |
| Change semantics | CRM closes a version (`valid_to`) and inserts a new one; 1,290 customers closed |
| Schema | id, tax id/code, name, address parts, region/district, lon/lat, ship-to string, `valid_from/valid_to` (epoch), `units_purchased`, `loyalty_segment` |
| Known issues | text `'NULL'`, `'NA NA'`; `46506.0`-style postcodes and numbers; inconsistent `region`/`district` coding |

## 6. Data access, security and secrets 🟥

Status: agreed day 1 (dev); prod items open until a service principal exists

* Dev deploys as the developer; prod as a service principal with `USE CATALOG`, `CREATE SCHEMA/VOLUME/TABLE`, `MODIFY` on the catalog.
* Secrets only in scope `retail-platform-<env>`; key names are the contract (§5); rotate anything ever pasted into a notebook.
* Consumers: `SELECT` on `gold` only (`platform.grants` in `gold.json`); Bronze/Silver for engineering and stewards.
* Masking of PII columns for non-`pii_readers` groups planned for v2; tags in place from v1.

## 7. Confidentiality classification 🟥

Owner: data steward · Status: agreed day 1, columns confirmed after profiling

| Dataset | Classification | Columns | Handling |
| --- | --- | --- | --- |
| customers (all layers) | PII | `tax_id`, `customer_name`, `first_name`, `last_name`, `street`, `number`, `unit`, `ship_to_address`, `lon`, `lat` | tag `classification=pii`; propagate Bronze→Silver→Gold (contract test); mask in v2 |
| sales_orders, sales | PII | `customer_name` | same |
| everything else | internal | | tag `classification=internal` |
| raw volume | internal, contains PII | | engineering access only; retention per policy |

## 8. Data refresh, latency and SLAs 🟧

Status: agreed after discovery

| Item | Value |
| --- | --- |
| Frequency | daily batch |
| Source ready | POS file by 04:00 UTC; CRM and Cosmos continuously available |
| Job start / Gold ready | 05:00 UTC start; Gold by 06:00 UTC (duration alert at 60 min) |
| Backfill | any past date via `run_date`; SCD2 entities chronologically |
| Late data | full extracts → a late row appears on the next day's load and is versioned |
| Business time zone | UTC |

## 9. Data quality rules 🟧

Status: first draft after profiling (Class 8); severities agreed with the analytics lead

| Layer | Entity | Rule | Type | Severity | On failure |
| --- | --- | --- | --- | --- | --- |
| Bronze | sales_orders | order_number, customer_id not null; ≥1 row | not_null, min_row_count | error | quarantine |
| Bronze | sales_orders | order_number unique | unique | warn | — (versions expected) |
| Bronze | sales | customer_id, product payload not null; total ≥ 0; ≥1 row | not_null, range, min_row_count | error | quarantine |
| Bronze | customers | id, name not null; valid_from not null; ≥1,000 rows | not_null, expression, min_row_count | error / warn | quarantine |
| Silver | sales_orders | lines parsed (>0); order_ts present; line count consistent | expression, not_null | error / warn / warn | fail |
| Silver | sales | product parsed; amount = price × qty; amount ≥ 0 | expression, range | error / warn / error | fail |
| Silver | customers | id unique after dedupe; state format; segment 0–3; coordinates in range | unique, regex, accepted_values, expression | error / warn | fail |
| Gold | facts | PK unique; net_amount ≥ 0; customer resolved | unique, range, expression | error / warn | fail |

Reconciliation: lines net = headers net; lines gross = Silver gross; zero FK orphans.
Accepted caveats: 1% orders without timestamp; derived brand; web/store not row-linkable.

## 10. Conceptual model 🟧

Status: agreed in discovery

A **customer** places **orders** online; an order contains **order lines**, each for one
**product**, possibly with a **promotion**. A customer also makes **store sales**, each
for one product. Products and customers are shared across channels; sales happen on a
**date**.

## 11. Logical model 🟧

Status: agreed before Gold build (Class 13)

| Entity | Business key | History | Relationships |
| --- | --- | --- | --- |
| Customer | customer_id | SCD2 (versions) | 1 → many orders, many POS sales |
| Product | product_id | SCD1 (derived) | 1 → many order lines / POS sales |
| Date | date | static | 1 → many facts |
| Order | order_number | SCD2 in Silver; current in Gold | 1 → many order lines |
| Order line | order_number + line_number | SCD1 (`silver.sales_order_lines`) | many → 1 customer, product, promotion, date |
| Order click | order_number + product_id | SCD1 (`silver.sales_order_clicks`) | many → 1 order |
| Promotion | promo_id | derived | 1 → many order lines |
| POS sale | sale_id (hash) | SCD1 | many → 1 customer, product, date |

## 12. Target design — Gold 🟧

Status: agreed before build; metric views added when AI needs were confirmed

| Object | Type | Grain | Keys | Measures / attributes |
| --- | --- | --- | --- | --- |
| dim_date | dim | day | date_key | calendar attributes |
| dim_customer | dim (SCD2) | customer version | customer_sk PK; customer_id | name, type, geography, loyalty, is_active, is_current; Unknown = −1 |
| dim_product | dim | product | product_sk PK; product_id | name, brand, brand_source; Unknown = −1 |
| dim_promotion | dim | promotion code | promo_id PK (NONE member) | promotion_name, discount_rate |
| fact_sales_order_line | fact | order × line | order_line_id PK; FKs customer_sk, product_sk, promo_id, order_date_key | quantity, unit_price, gross, discount, net |
| fact_sales_order | fact | order | order_number PK; FKs customer_sk, order_date_key | lines, units, amounts, clicks, has_promotion |
| fact_pos_sale | fact | product sold | sale_id PK; FKs | quantity, unit_price, total_amount |
| customers_current, *_obt | views | — | — | current state; one-big-table |
| mv_web_sales, mv_pos_sales | metric views | — | — | governed measures (§3) |

## 13. Physical model 🟩 (filled during build — Classes 6, 7, 12, 19)

Catalog `retaildataplatform`; schemas `bronze`, `silver`, `gold`, `ops`; volume
`bronze.raw_data`. Delta everywhere; audit columns `_load_date/_ingested_at/_run_id/_source_system/_source_file`
in Bronze; `_row_hash`, lineage and SCD2 columns in Silver; `_gold_updated_at/_run_id`
in Gold. Bronze written with `replaceWhere _load_date`, Silver with `MERGE`, Gold full
rebuild. Liquid clustering on load date + keys / business keys / surrogate keys. CDF,
deletion vectors, auto-optimise on. PK/FK constraints in Gold. One catalog per
environment via variable.

## 14. Ingestion and landing 🟧

Raw volume `bronze.raw_data/<source>/load_date=YYYY-MM-DD`; S3 copied byte-for-byte via
the Files API, Cosmos serialised to JSON, SQL Server to CSV; folder replaced on re-run;
additive schema evolution allowed, type changes fail; Serverless: pip libraries only,
no JVM APIs, no caching, `try_cast` for ANSI mode.

## 15. Orchestration 🟩 (Class 20)

One job: `land_cosmos`, `land_s3`, `land_sqlserver` in parallel → `ds2b` (`ALL_SUCCESS`)
→ `b2s` → `s2g`. Parameters `run_date` (`{{job.start_time.iso_date}}`), `environment`,
`catalog`, `secret_scope`. Landing tasks retry twice; 05:00 UTC schedule (paused in dev);
2-hour timeout; failure and duration e-mails; weekly maintenance task planned.

## 16. Observability and operations 🟩 (Class 17, 25)

`ops.pipeline_runs` (status, rows, duration, error per run/task/entity),
`ops.data_quality_results`, JSON logs with run id; SQL alerts on error-severity failures
and Gold freshness > 26 h; runbook scenarios in `docs/operations.md`.

## 17. Consumption 🟩 (Class 24)

| Consumer | Tool | Entry point |
| --- | --- | --- |
| Finance, BI | Power BI via SQL warehouse | star schema, relationships from PK/FK, `dim_date` as date table, `mv_*` |
| AI | Genie space / Assistant / notebooks | OBT views, metric views, comments, CDF |
| Analysts | SQL editor | `docs/consumers.md` example queries |

## 18. Data dictionary 🟩

Generated from the three configs by `python -m common_utils.gold` into
`docs/data-dictionary.md`; CI fails if stale.

## 19. Lineage 🟩

Row level: `_run_id` → `ops.pipeline_runs`; Silver `_load_date` → Bronze partition →
`_source_file` → raw bytes. System level: Unity Catalog lineage on the Delta tables.

## 20. Environments, deployment and testing 🟩 (Classes 21–23)

Targets `dev` (development mode) and `prod` (production, service principal). Bundle
deploys from CI on `main`; PRs run ruff, 39 tests (unit, contract, end-to-end on
fixtures), dictionary check and `bundle validate`; integration tests run in the workspace.

## 21. Traceability 🟩

| Requirement | Decision | Artefact | Proof |
| --- | --- | --- | --- |
| §5.1 orders re-emitted | SCD2 on order_number, latest `_id` wins | `silver.json`, `scd.py` | `test_silver_transformations_on_fixtures` |
| §5.2 no POS key | `sale_id` hash | `silver.json` | same |
| §5.3 CRM versions | latest `valid_from` per customer + SCD2 | `silver.json` | same |
| §3 Q1–Q6 | star schema + metric views + OBT | `gold.json` | `test_gold_keys_and_referential_integrity`, reconciliation test |
| §7 classification | `pii_columns` propagated | configs, `governance.py` | `test_pii_columns_declared_in_bronze_stay_declared_downstream` |
| §8 SLA | schedule, parallel landing, duration alert | `jobs.yml` | run history |
| §9 rules | quality engine | `quality.py` | `test_evaluate_rules_counts_failures_per_rule` |
| §6 secrets | secret scope keys in config | `bronze.json` | `test_no_credential_literal_is_committed` |
| §14 Serverless | pymongo/boto3/Files API, no cache, try_cast | `sources.py`, `silver.py` | ADR 0002, `test_tolerant_cast…` |
| §20 portability | variables + `${catalog}` | `databricks.yml`, `gold.py` | `test_gold_sql_only_references_silver_or_gold_objects` |

## 22. Risks, assumptions, open questions and change log 🟥

| Date | Type | Description | Owner | Resolution |
| --- | --- | --- | --- | --- |
| day 1 | assumption | `order_number` is unique | DE | **wrong** — profiling found re-emits → SCD2 (changed §4, §11) |
| day 1 | assumption | POS export has a transaction id | DE | **wrong** → hash key (changed §5.2, §11) |
| day 1 | risk | Azure SQL unreachable from Serverless | CRM admin | firewall + driver options; documented in runbook |
| day 2 | question | which discount applies to a line? | analytics lead | line-level `promotion_info` where `promo_item` = product |
| day 3 | question | are metric views available in Free Edition? | DE | soft-fail with warning; strict flag for prod |
| open | risk | S3 access keys instead of IAM role | security | v2 storage credential |
| open | question | production service principal and workspace | sponsor | needed before `deploy -t prod` |

## 23. Acceptance and sign-off 🟩

Q1–Q6 answered in Power BI, SQL and Genie; reconciliation passes on every run; Gold
ready before 06:00 UTC for five consecutive days; documentation current; Finance and
sponsor sign-off recorded here with dates.

---

## Homework

1. Take the blank template and fill §1, §2 and §5 for a company you know (or invent one). Mark every field you cannot know yet as `unknown` and write who you would ask.
2. Pick one row of §22 and explain what would have gone wrong if we had built on the assumption.
3. Fill §5 for a *returns* feed for this retailer (capstone, Class 26).
