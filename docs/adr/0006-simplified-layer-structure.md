# ADR 0006 – One folder per layer, and a generic common_utils

**Status**: accepted (2026-09), supersedes the config-interpreter parts of ADR 0001

## Context
The previous structure put every notebook in `src/`, every declaration in
`src/config/{bronze,silver,gold}.json`, and every behaviour in a library that
interpreted those files. Three problems followed:

1. **The business logic was in strings.** A Gold table was a 1,263-character SQL
   statement inside a JSON value: no syntax highlighting, no diff worth reading, no way
   to run it by hand.
2. **The library knew the project.** `common_utils` contained `silver.py`, `gold.py`
   and `sources.py` written around this domain, so none of it could be lifted into the
   next project without carrying retail with it.
3. **Reading it required holding two files in your head.** To answer "what happens to
   customers?" you read a JSON entry, then the generic interpreter that consumed it.

## Decision
* Each layer owns a folder with `code/` and, where it earns its place, `config/`. The
  four medallion layers live under `src/` (`src/ingestion/`, `src/bronze/`,
  `src/silver/`, `src/gold/`), which keeps the pipeline separate from the repository
  furniture — docs, classes, tests, tools. `quality/` and `governance/` stay at the
  root because they are cross-cutting concerns, not steps in the load.
* Silver is one notebook per entity, explicit PySpark, top to bottom.
* Gold is numbered `.sql` files.
* Quality and governance are separate notebooks and separate tasks, not steps folded
  into the load. Observability stays inline, because a run record is written *by* the
  step it describes.
* `common_utils` holds only what any project in any domain could use: `logger`,
  `metadata`, `ingestors`, `writers`, `transforms`, `scd`, `quality`, `governance`,
  `observability`, `settings`. A test (`test_common_utils_stays_generic`) fails if a
  domain word appears in it.
* The audit columns drop from seven to three (`_load_date`, `_ingested_at`,
  `_source_file`); run id, environment and task belong to `ops.pipeline_runs`.

## Consequences
* About 900 lines of interpreter code were deleted, and the layer folders got longer —
  that trade is deliberate: the length is now in the part you actually read.
* A new engineer can be shown one Silver notebook and understand the whole layer.
* Repetition across the three Silver notebooks is accepted where the entities differ;
  anything genuinely shared moves into `common_utils` rather than into a config schema.
* `common_utils` can be copied into the next project unchanged, which is the whole point.
