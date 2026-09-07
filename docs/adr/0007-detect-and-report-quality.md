# ADR 0007 – Data quality detects and reports; it does not block

**Status**: accepted (2026-09)

## Context
The earlier design let quality rules fail an entity and route failing rows into
`<entity>_quarantine` tables. In practice this made a bad extract into an outage: the
load stopped, Gold went stale, and the quarantine tables were a second copy of the data
that nobody reconciled or cleaned up.

## Decision
Quality is one task that runs **after** Gold. It evaluates every rule and every
cross-table reconciliation, writes one row per result to `ops.data_quality_results`,
and never fails the job. There are no quarantine tables.

## Consequences
* This is safe because of how the layers write. Silver merges on a business key, so an
  extract with broken rows merges nothing rather than corrupting a table, and Gold is a
  full rebuild from Silver. Blocking the load would replace a reported problem with an
  outage.
* Alerting moves to where it belongs: a SQL alert on `ops.data_quality_results` for
  `severity = 'error'`. Setting that alert up is a production checklist item, not an
  optional extra — without it, nobody reads the results table.
* The reconciliation checks (line totals vs header totals, Gold revenue vs Silver
  revenue, orphan keys) are the most valuable rules in the file, because they are the
  ones a schema check cannot catch.
* If a feed ever needs a hard gate, add a task between Silver and Gold that reads the
  results table and fails — the results are already there.
