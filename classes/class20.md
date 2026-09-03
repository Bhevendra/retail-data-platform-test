# Class 20 — Orchestration with Databricks Jobs (jobs.yml)

## Objectives

* Read and write YAML.
* Describe a job as tasks with dependencies, parameters, retries, a schedule and notifications.
* Explain how job parameters flow into notebook widgets and why `run_date` is a job parameter.
* Define Serverless environments (Python dependencies) for the job.

## Time plan (95 min)

| Min | Segment |
| --- | --- |
| 0–15 | Build the job by clicking in the UI (once) — then explain why we will never click again |
| 15–30 | Mini-lesson: YAML |
| 30–60 | `resources/jobs.yml` line by line, written incrementally |
| 60–75 | Parameters and widgets; `{{job.start_time.iso_date}}`; backfill |
| 75–90 | Retries, timeouts, schedule, notifications, health rules, permissions |
| 90–95 | Homework |

## UI first (15 min)

Create a job in the UI with two tasks (`land_sqlserver` → `ds2b`) and run it. Then ask:
how do you copy this to prod? How do you review a change? How do you know what changed
last month? Answer: the job is *code* — `resources/jobs.yml` — deployed by a bundle (Class 21).

## Mini-lesson: YAML (15 min)

```yaml
name: retail-data-platform-dev      # key: value
tags:                                # nested mapping
  owner: data-engineering
tasks:                               # list of mappings
  - task_key: land_cosmos
    depends_on: []
  - task_key: ds2b
    depends_on:
      - task_key: land_cosmos
```

Indentation is structure; `-` is a list item; strings rarely need quotes except values
like `"false"` that must stay strings. Show `yaml.safe_load` in Python turning it into
dicts and lists — the same shapes as our JSON config.

## `jobs.yml` incrementally (30 min)

Write it in five passes, validating in between (`databricks bundle validate` — Class 21
sets up the CLI; today the instructor validates):

1. **Skeleton**: `resources.jobs.retail_data_platform` with `name`, `tags`, `max_concurrent_runs: 1`.
2. **Environment**: `environments` with `environment_key: platform` and the pip dependencies (`pymongo`, `boto3`, `databricks-sdk`). Serverless installs them once per run; the `%pip` line in notebooks stays for interactive use.
3. **Tasks**: three `land_*` tasks using the *same* notebook with different `source_name` base parameters; `ds2b` depending on all three with `run_if: ALL_SUCCESS`; `b2s`; `s2g`. Draw the DAG.
4. **Parameters**: job-level `run_date`, `environment`, `catalog`, `secret_scope` — they fill the widgets of every task with the same name.
5. **Operations**: `max_retries`/`min_retry_interval_millis` per task, `timeout_seconds`, `schedule` (`0 0 5 * * ?` UTC, `pause_status` from a variable), `email_notifications.on_failure`, `health.rules` duration threshold, `permissions`.

Compare the result with the reference file; discuss each choice ("why `ALL_SUCCESS`?"
— a partial day must never load silently; "why retries on landing but not on gold?" —
network flakiness vs deterministic rebuild).

## Parameters and backfill (15 min)

`run_date: "{{job.start_time.iso_date}}"` is a Databricks template: the job's start date.
`parse_run_date` in `runtime.py` handles that value, an empty string, or an explicit date.
Backfill therefore needs no code:

```bash
databricks bundle run retail_data_platform -t dev --params run_date=2026-09-01
```

Explain why `run_date` must be a *job* parameter rather than each task computing
`date.today()`: a run that crosses midnight would land on one day and load another.

## Homework

1. Add a `notification` for `on_success` to your own email and remove it again (discuss alert fatigue).
2. Draw the DAG for a fourth source and write its task block.
3. Read the Databricks docs page on job parameters and list three other `{{...}}` values.

## Common problems

* Notebook path is relative to the YAML file: `../src/...`.
* `environment_key` must be declared under `environments`; a typo fails validation with a clear message.
* Widgets created with `dbutils.widgets.text(name, default)` are *overridden* by job parameters of the same name — the default only applies interactively.
