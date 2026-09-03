# Operations runbook

## One-time setup per workspace

1. **Secret scope** named `retail-platform-<target>` (`secret_scope` variable):

   | Source | Secret keys |
   | --- | --- |
   | Cosmos DB | `cosmos-connection-string` |
   | SQL Server | `sqlserver-username`, `sqlserver-password` |
   | S3 | `aws-access-key-id`, `aws-secret-access-key` |

   Never commit values; `tests/test_contracts.py` fails on credential-looking literals.
2. **Unity Catalog grants** for the deploying identity: `USE CATALOG`, `CREATE SCHEMA`,
   `CREATE VOLUME`, `CREATE TABLE`, `MODIFY` on `retaildataplatform`. In production the
   catalog, schemas and external locations should be provisioned by a platform-admin
   bundle; this job only creates them `IF NOT EXISTS` for convenience.
3. **Azure SQL networking**: allow Databricks Serverless egress IPs on the server firewall.
   Serverless connects to Azure SQL only with `trustServerCertificate=true` and a long
   `loginTimeout` (set as `jdbc_options` in `bronze.json`); encryption stays on.
4. **Production only**: a service principal (`run_as`), the `alert_email` distribution
   list, and `SELECT` grants on `gold` for BI/AI groups via `platform.grants` in
   `gold.json`, e.g. `{"SELECT": ["bi_readers", "ai_engineers"]}`.

## Daily operation

* The job runs at 05:00 UTC (paused in dev). Landing tasks run in parallel; `ds2b`
  waits for all three.
* **Where to look first**: `ops.pipeline_runs` (status, rows, duration, error per
  entity) and `ops.data_quality_results` (which rule failed, how many rows).

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
FROM retaildataplatform.ops.pipeline_runs WHERE status = 'SUCCEEDED' AND layer = 'gold' GROUP BY entity;
```

## Common scenarios

| Scenario | Action |
| --- | --- |
| Source export was late / wrong | Re-run the job with `--params run_date=<date>`; every layer replaces that date's data. |
| One source failed, the others succeeded | `ds2b` still loads the healthy sources and raises at the end. Fix the source, then run `ds2b` with widget `sources=<name>` and continue with `b2s`, `s2g`. |
| Rows quarantined | Inspect `bronze.<entity>_quarantine` (`_failed_rules` lists the violated rules). Fix upstream or relax the rule via PR; re-run the date. |
| Blocking `unique` / `min_row_count` failure | These cannot be quarantined row by row and stop the entity. Usually a duplicate export or an empty extract. |
| Need to rebuild Silver history | Drop the Silver table, then re-run `b2s` for each `run_date` in Bronze in chronological order (`SELECT DISTINCT _load_date FROM bronze.<entity> ORDER BY 1`). |
| Schema change upstream | Additive columns flow through automatically (`mergeSchema` in Bronze, `MERGE WITH SCHEMA EVOLUTION` in Silver). Type changes fail loudly: add a `cast` in `silver.json`. |
| Deleted keys in a full extract | Run `b2s` with widget `detect_deletes=true` (or set it in `jobs.yml`) to close SCD2 rows whose key vanished. |
| Metric views not created | They are a preview feature; failures are logged as warnings unless `strict_metric_views` is `true` in `gold.json`. |

## Backfill

```bash
for d in 2026-09-01 2026-09-02 2026-09-03; do
  databricks bundle run retail_data_platform -t dev --params run_date=$d
done
```

Bronze and Silver are date-partitioned by `_load_date`, so out-of-order backfills are
safe for SCD1 entities; SCD2 entities should be backfilled chronologically.

## Cost and performance

* Serverless: no cluster to size. Row counts and durations in `ops.pipeline_runs` show
  where time goes.
* Bronze/Silver use liquid clustering on `_load_date` and the business key; Gold on the
  surrogate key. `OPTIMIZE` runs automatically via auto-compaction; run `VACUUM` weekly
  according to your retention policy.
* Change data feed is enabled on all tables, so downstream incremental consumers
  (feature stores, reverse ETL) can read only changes.

## Production controls checklist

- [ ] Dedicated service principal deploys and runs the job; personal tokens are not used.
- [ ] Consumers have `SELECT` on `gold` only; Bronze/Silver stay with data engineering.
- [ ] Alerts wired to `alert_email` (job failure, duration SLA) and a SQL alert on
      `ops.data_quality_results` for `severity = 'error'`.
- [ ] Raw volume retention and VACUUM schedule agreed with the data owner.
- [ ] Upstream credentials that were ever exposed in reference notebooks are rotated.
- [ ] S3 access moves from access keys to a Unity Catalog storage credential / IAM role.
