# ADR 0004 – Gold as a star schema with a Unity Catalog semantic layer

**Status**: accepted (2026-09)

## Context
Gold previously exposed thin views over Silver. BI tools could not infer joins, SCD2
semantics leaked to analysts, and LLM agents lacked descriptions and measures.

## Decision
Gold materialises conformed dimensions (with version-level surrogate keys), facts at a
declared grain, a generated date dimension, informational PK/FK constraints, comments
and tags. Views provide current-state and one-big-table access; metric views define
governed measures.

Each Gold object is one numbered `.sql` file in `src/gold/sql/`, holding a complete
`CREATE OR REPLACE TABLE|VIEW ... AS` statement with `${catalog}` / `${silver}` /
`${gold}` placeholders. The runner executes them in filename order, so the number *is*
the dependency order — dimensions before facts, facts before the views that read them.

## Consequences
* Gold SQL is readable, diffable and testable: the test suite runs every file against
  fixture Silver tables on local Spark and asserts keys, orphans and reconciliation.
* A full rebuild each run keeps the model deterministic; move facts to an incremental
  MERGE over the Silver change data feed once volumes require it.
* Constraints are informational in Unity Catalog; the quality layer verifies them.
* Metric views are a preview feature, so those two files are named `.optional.sql` and
  are allowed to fail without failing the run.
