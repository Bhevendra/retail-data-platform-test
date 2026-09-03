# ADR 0004 – Gold as a star schema with a Unity Catalog semantic layer

**Status**: accepted (2026-09)

## Context
Gold previously exposed thin views over Silver. BI tools could not infer joins, SCD2
semantics leaked to analysts, and LLM agents lacked descriptions and measures.

## Decision
Gold materialises conformed dimensions (with version-level surrogate keys), facts at a
declared grain, a generated date dimension, PK/FK constraints, comments and tags. Views
provide current-state and one-big-table access; metric views define governed measures.
Products are built in dependency order from `gold.json`.

## Consequences
* Full rebuild each run keeps the model deterministic; move facts to incremental MERGE
  via Silver change data feed once volumes require it.
* Constraints are informational in Unity Catalog; the quality layer enforces them.
