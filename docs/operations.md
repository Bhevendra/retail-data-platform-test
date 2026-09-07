# Operations runbook

## One-time setup per workspace

1. **Secret scope** named `retail-platform-<target>` (the `secret_scope` variable):

   | Source | Secret keys |
   | --- | --- |
   | Cosmos DB | `cosmos-connection-string` |
   | SQL Server | `sqlserver-username`, `sqlserver-password` |
   | S3 | `aws-access-key-id`, `aws-secret-access-key` |

   Never commit values; the key names live in `ingestion/config/*.json` and a test
   fails on credential-looking literals.
2. **Unity Catalog grants** for the deploying identity: `USE CATALOG`, `CREATE SCHEMA`,
   `CREATE VOLUME`, `CREATE TABLE`, `MODIFY` on the target catalog. In production the
   catalog, schemas and external locations should be provisioned by a platform-admin
   bundle; this job only creates them `IF NOT EXISTS` for convenience.
3. **Azure SQL networking**: allow Databricks Serverless egress IPs on the server
   firewall. Serverless connects only with `trustServerCertificate=true` and a long
   `loginTimeout` (set in `ingestion/config/sqlserver_customers.json`); encryption
   stays on.
4. **Production only**: a service principal (`run_as`), the `alert_email` distribution
   list, and `SELECT` grants on `gold` for BI/AI groups via `grants` in
   `governance/config/tables.json`, e.g. `{"SELECT": ["bi_readers", "ai_engineers"]}`.

## Daily operation

The job runs at 05:00 UTC (paused in dev). The three ingestion tasks run in parallel;
`bronze` waits for all of them, the three Silver tasks run in parallel, `gold` waits
for all of them, then `quality` and `governance` finish the run.

**Where to look first**: `ops.pipeline_runs` and `ops.data_quality_results`.

```sql
-- Today's run at a glance
SELECT layer, entity, status, rows_read, rows_written, duration_seconds, error_message
FROM retaildataplatform.ops.pipeline_runs
WHERE run_date = current_date() ORDER BY started_at;

-- Quality failures in the last 7 days
SELECT run_date, layer, entity, rule_name, severity, failed_rows
FROM retaildataplatform.ops.data_quality_results
WHERE NOT passed AND run_date >= current_date() - 7 ORDER BY run_date DESC;

-- Freshness check for consumers
SELECT entity, max(finished_at) AS last_success
FROM retaildataplatform.ops.pipeline_runs
WHERE status = 'SUCCEEDED' AND layer = 'gold' GROUP BY entity;
```

## Common scenarios

| Scenario | Action |
| --- | --- |
| Source export was late or wrong | Re-run the job with `--params run_date=<date>`; every layer replaces that date's data. |
| One ingestion task failed | `bronze` will not run (`ALL_SUCCESS`), so nothing partial is loaded. Fix the source and re-run the job for that date, or repair the run from the failed task in the Jobs UI. |
| A quality rule failed | The run still succeeded — that is the design. Read `ops.data_quality_results` to see which rule and how many rows, then decide: fix upstream, fix the transformation, or change the rule via PR. |
| A reconciliation failed | This is the serious one: totals no longer agree between layers. Compare `silver.sales_order_lines` against `silver.sales_orders` for the affected date before touching Gold. |
| Need to rebuild Silver history | Drop the Silver table, then re-run its notebook for each `run_date` in Bronze in chronological order (`SELECT DISTINCT _load_date FROM bronze.<entity> ORDER BY 1`). |
| Schema change upstream | Additive columns flow through automatically (`mergeSchema` in Bronze, `MERGE WITH SCHEMA EVOLUTION` in Silver). Type changes need a `cast_columns` entry in the Silver notebook. |
| Deleted keys in a full extract | Run `silver_customers` with widget `detect_deletes=true` (or set it in `jobs.yml`) to close SCD2 rows whose key vanished. |
| Metric views not created | They are a preview feature. The `.optional.sql` files log a warning and are skipped if the workspace does not support them. |

## Running one step by hand

Every notebook has widgets (`catalog`, `bronze_schema`, `silver_schema`, `run_date`,
and a few task-specific ones such as `detect_deletes` or `config_path`), so any single
step can be run for any date from the notebook UI without touching the job.

## Backfill

```bash
for d in 2026-09-01 2026-09-02 2026-09-03; do
  databricks bundle run retail_data_platform -t dev --params run_date=$d
done
```

Bronze and Silver read by `_load_date`, so out-of-order backfills are safe for SCD1
entities; SCD2 entities (customers, order headers) must be backfilled chronologically,
otherwise the version history is built in the wrong order.

## Cost and performance

* Serverless: no cluster to size. Row counts and durations in `ops.pipeline_runs` show
  where time goes.
* Bronze and Silver cluster on `_load_date` and the business key, Gold on the surrogate
  key. Auto-compaction handles `OPTIMIZE`; run `VACUUM` weekly per your retention
  policy.
* Change data feed is on for every table, so incremental consumers (feature stores,
  reverse ETL) can read only what changed.

## Production controls checklist

- [ ] A dedicated service principal deploys and runs the job; personal tokens are not used.
- [ ] Consumers have `SELECT` on `gold` only; Bronze and Silver stay with data engineering.
- [ ] Alerts wired to `alert_email` (job failure, duration SLA) **and** a SQL alert on
      `ops.data_quality_results` for `severity = 'error'` — with detect-and-report
      quality, that alert is the safety net.
- [ ] Raw volume retention and the VACUUM schedule agreed with the data owner.
- [ ] Upstream credentials that were ever exposed in reference notebooks are rotated.
- [ ] S3 access moved from access keys to a Unity Catalog storage credential / IAM role.
