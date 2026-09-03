# ADR 0003 – Idempotent loads keyed by run_date

**Status**: accepted (2026-09)

## Context
The original Bronze step appended on every run, so retries duplicated data and
Silver hashes could not distinguish reloads from real changes.

## Decision
Every layer is keyed by the job parameter `run_date`: the landing folder, the Bronze
`_load_date` (written with `replaceWhere`), the Silver batch filter, and the Gold rebuild.
Silver change detection uses a null-safe hash over business columns only.

## Consequences
* Any date can be re-run or backfilled without cleanup; retries are safe.
* SCD2 backfills must be chronological (history is built from ordered batches).
