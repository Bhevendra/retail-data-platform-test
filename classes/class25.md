# Class 25 — Enterprise readiness

Everything that turns a working pipeline into something an organisation can own: docs,
runbooks, decision records, alerting, maintenance, security hardening, and the checklist
for a production deployment.

## Objectives

* Write an ADR and a runbook entry; explain why decisions are documented next to code.
* Configure alerting on failed quality rules and freshness.
* Add maintenance (OPTIMIZE / VACUUM) and retention.
* Harden security: service principal, PII masking, storage credentials instead of keys.
* Walk the production checklist and identify what is still missing from the reference repo (the honest list).

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–20 | Documentation as a deliverable: README, architecture, consumers, operations, ADRs |
| 20–40 | Alerting: SQL alerts on `ops.data_quality_results` and freshness; job notifications recap |
| 40–55 | Maintenance: OPTIMIZE, VACUUM, retention of the raw volume; a maintenance task in the job |
| 55–75 | Security: service principal + `run_as`, column masks on PII tags, UC storage credential for S3 |
| 75–90 | Production checklist and the gap list |
| 90–100 | Homework |

## Documentation (20 min)

Read the four docs and say who each is for: README (new engineer), `architecture.md`
(architect/reviewer), `consumers.md` (BI/AI users), `operations.md` (on-call). Then ADRs:
title, status, context, decision, consequences. Students write ADR 0005 in class for a
decision they made differently in their own build (e.g. the brand fallback order).

## Alerting (20 min)

```sql
-- SQL alert: any blocking rule failed today
SELECT count(*) AS failures FROM retaildataplatform.ops.data_quality_results
WHERE run_date = current_date() AND severity = 'error' AND NOT passed;

-- SQL alert: Gold not refreshed in 26 hours
SELECT timestampdiff(HOUR, max(finished_at), current_timestamp()) AS hours_since_success
FROM retaildataplatform.ops.pipeline_runs WHERE layer = 'gold' AND status = 'SUCCEEDED';
```

Create them in Databricks SQL Alerts with a schedule; discuss thresholds and who
receives what (job failure → engineers; freshness → engineers and BI owner).

## Maintenance (15 min)

```sql
OPTIMIZE retaildataplatform.silver.customers;
VACUUM retaildataplatform.silver.customers RETAIN 168 HOURS;
```

Auto-compaction is on (table properties), so OPTIMIZE is rarely needed; VACUUM removes
old files beyond the retention window — after which time travel and CDF beyond that
window stop working. Add a weekly `maintenance` task to `jobs.yml` (students write it)
and a raw-volume retention rule (`dbutils.fs.rm` of `load_date` folders older than N days,
agreed with the data owner).

## Security hardening (20 min)

* **Service principal**: create it, grant catalog privileges, put its token in the `prod`
  GitHub environment, uncomment `run_as` in `databricks.yml`.
* **Column masks**: a SQL function and `ALTER TABLE … ALTER COLUMN … SET MASK` on every
  `classification = pii` column in Gold, exempting a `pii_readers` group. Show how the tag
  query from Class 19 drives which columns get masks — governance metadata becomes
  enforcement.
* **Storage credential**: replace AWS access keys with a Unity Catalog external location
  and IAM role; `land_s3_object` then becomes a `dbutils.fs.cp` from the external location.
* **Secret rotation**: the runbook says credentials from the original reference notebooks
  must be rotated; explain why exposure in git history is permanent.

## Production checklist and the honest gap list (15 min)

Checklist from `docs/operations.md`. Then what the reference repo does *not* yet have —
students should be able to say this in an interview:

* freshness as a *quality rule* rather than an alert; source-to-target row-count reconciliation stored as a rule result;
* schema-drift detection with explicit approval rather than silent additive evolution;
* column masks actually attached (only tags exist);
* a `deploy-prod` CI job and a real service principal;
* Lakehouse Monitoring / a dashboard over `ops.*`;
* integration tests in CI (needs a workspace token in CI and a scratch schema).

Assign each gap to a pair for the capstone stretch goals.

## Homework

1. Write the `maintenance` task and the SQL alert; deploy to dev.
2. Draft ADR 0005.
3. Pick one gap from the list and write the plan (files to change, tests to add) in half a page.

## Common problems

* VACUUM with a retention below 7 days needs a safety flag — do not turn it off casually.
* Masks on views: apply to the table; views inherit.
* A service principal without `USE CATALOG` produces confusing "table not found" errors — grants first.
