# Class 21 — Databricks Asset Bundles (databricks.yml)

## Objectives

* Install and authenticate the Databricks CLI.
* Explain a bundle: `databricks.yml` (targets, variables, sync) + `resources/*.yml`.
* Run `validate`, `deploy`, `run` for dev; explain how prod differs (mode, service principal, paused schedule).
* Explain why every hard-coded name in config became a variable or placeholder.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–15 | CLI setup and `databricks auth login` |
| 15–40 | `databricks.yml` line by line |
| 40–60 | `validate` → `deploy` → `run`; inspect what was deployed |
| 60–75 | Targets: dev vs prod; `mode: development` behaviours |
| 75–90 | "Deploy anywhere": catalog variable, `${catalog}` placeholders, secret scope per target |
| 90–95 | Homework |

## CLI (15 min)

```bash
databricks --version
databricks auth login --host https://<workspace-url>
databricks current-user me
```

## `databricks.yml` (25 min)

Read the reference file top to bottom:

* `bundle.name` — everything deployed is prefixed with it.
* `include: resources/*.yml` — jobs live in separate files.
* `sync.include` — only `src/**` and `common_utils/**` are uploaded (tests and docs stay local).
* `variables` — `catalog`, `secret_scope`, `alert_email`, `schedule_pause_status`, `run_as_service_principal` with descriptions and defaults.
* `targets.dev` — `mode: development`, `default: true`, workspace host, `secret_scope: retail-platform-dev`.
* `targets.prod` — `mode: production`, `root_path`, `schedule_pause_status: UNPAUSED`, commented `run_as`.

Variables are referenced as `${var.catalog}` in `jobs.yml`; `${bundle.target}` gives the
target name (used in the job name and the `environment` parameter).

## Deploy and run (20 min)

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_data_platform -t dev
```

After deploy, open the workspace: files under `/Workspace/Users/<you>/.bundle/retail-data-platform/dev/files`,
a job named `[dev <you>] retail-data-platform-dev`. Explain the `[dev …]` prefix and
the paused schedule: development mode isolates each developer. Watch the run; open
`ops.pipeline_runs`.

Then change a column comment in `gold.json`, deploy again, run `s2g` only — show that a
config change is a deploy, not a manual edit in the workspace.

## Targets (15 min)

| | dev | prod |
| --- | --- | --- |
| mode | development (prefixes, paused schedule, deploying user) | production (exact names, schedule on) |
| identity | you | service principal (`run_as`) |
| secrets | `retail-platform-dev` | `retail-platform-prod` |
| catalog | variable, same default; could be `retaildataplatform_prod` | |

Discuss what has to exist before `deploy -t prod` works: the secret scope with the same
key names, the service principal with catalog grants, the workspace URL. That checklist is
`docs/operations.md`.

## Deploy anywhere (15 min)

Trace one value through the system: catalog name. It appears in `databricks.yml` as a
variable → job parameter → notebook widget → `RunContext.catalog` → `render_sql`
replacing `${catalog}` in Gold SQL. Not one file in `src/` hard-codes it in SQL (the
contract test `test_gold_sql_only_references_silver_or_gold_objects` fails if someone
types `retaildataplatform.` into `gold.json`). The same is true for schema names and the
secret scope. Ask the class to list what would change to run this for a different
retailer: three JSON files and the variables.

## Homework

1. Deploy to dev with `--var catalog=retaildataplatform_sandbox` and check which objects appear in the new catalog (ownership permitting).
2. Write the `prod` checklist in your own words, in order.
3. Explain the difference between `bundle deploy` and `bundle run`.

## Common problems

* `validate` errors about unknown fields — indentation in YAML.
* Deploying from a dirty working tree uploads local edits — always deploy from committed code (Class 23 makes CI do it).
* `run` uses the deployed job, not your local notebooks — redeploy after every change.
