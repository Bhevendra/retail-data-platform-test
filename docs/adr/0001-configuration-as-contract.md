# ADR 0001 – Configuration where it repeats, code where it differs

**Status**: accepted (2026-09), amended by ADR 0006

## Context
The platform ingests heterogeneous sources and serves many consumers. Hard-coding
per-source connection logic in notebooks made every new source a code change and hid
the data contract from reviewers. The first attempt at fixing this went the other way
and declared *everything* — transformations, SCD strategies, Gold SQL — in JSON, which
moved the business logic into strings that no tool could read (see ADR 0006).

## Decision
Configuration is used where the work genuinely repeats, and code where it genuinely
differs.

* **Config**: sources (`ingestion/config/*.json`), quality rules
  (`quality/config/rules.json`), governance metadata
  (`governance/config/tables.json`). These are lists of near-identical things.
* **Code**: the Silver transformations (one notebook per entity, explicit PySpark) and
  the Gold model (`gold/sql/*.sql`). These differ per entity and are the business logic.

## Consequences
* Adding a source is a JSON file plus a job task; contract tests catch a config with no
  task, a rule on a table nobody builds, or a lost PII tag.
* Changing how customers are cleaned is a diff in one notebook, readable in review,
  runnable in isolation.
* There is no config schema to validate transformations against — the tests and the
  notebooks themselves are the contract instead.
