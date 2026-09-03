# ADR 0001 – Configuration as the pipeline contract

**Status**: accepted (2026-09)

## Context
The platform ingests heterogeneous sources and serves many consumers. Hard-coding
per-source logic in notebooks made every change a code change and hid the data
contract from reviewers.

## Decision
All sources, rules, transformations, SCD strategies, Gold products, comments and
grants are declared in `src/config/*.json`. Typed dataclasses validate the files at
import time and in CI. Notebooks are thin and identical in shape; the library is generic.

## Consequences
* Adding a source or Gold product is a JSON PR; contract tests catch inconsistencies
  (unknown Bronze table, lost PII tags, missing job task).
* Business logic in SQL strings inside JSON is less ergonomic than `.sql` files; keep
  statements short and push complexity to Silver `derived` columns.
