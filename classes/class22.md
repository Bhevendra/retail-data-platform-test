# Class 22 — Testing from zero to the project's test suite

Dense; split at the natural break if needed.

## Objectives

* Write and run a pytest test; use `assert`, `pytest.raises`, `parametrize`, fixtures.
* Test pure functions (config validation) without Spark.
* Test DataFrame logic on a local Spark session (no Databricks).
* Explain the four test families in the repo: unit, contract, end-to-end on fixtures, integration on Databricks.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–15 | Why tests: the two production failures a test would have caught |
| 15–40 | Mini-lesson: pytest basics on `sum2no` and on `QualityRule.from_dict` |
| 40–60 | Local Spark fixture; test `row_hash`, `deduplicate`, `evaluate_rules` |
| 60–75 | Natural break — Contract tests: configs must agree with each other |
| 75–90 | End-to-end on synthetic fixtures; integration tests marked for Databricks |
| 90–100 | Homework |

## Why (15 min)

Two true stories from this project: the ANSI cast crash (Class 9) was invisible locally
because local Spark is lenient — the fix came with `test_tolerant_cast…`; the fixture
data caught a brand-derivation bug before it reached the workspace. Tests are the
cheapest place to be wrong.

Setup: `pip install -r requirements-dev.txt` (pytest, pyspark, ruff, pyyaml) and `pytest -q`
from the repo root. Run the suite once: 39 tests, ~20 seconds, no workspace needed.

## pytest basics (25 min)

```python
# tests/test_sum.py
def sum2no(a, b):
    return a + b

def test_sum2no_adds():
    assert sum2no(3, 4) == 7
```

Then failure output (change to `== 8`), then `pytest.raises`:

```python
import pytest
from common_utils.config import QualityRule, ConfigError

def test_rule_requires_column():
    with pytest.raises(ConfigError, match="requires 'column'"):
        QualityRule.from_dict({"name": "r", "type": "not_null"}, "entity")
```

Then `@pytest.mark.parametrize` — read `tests/test_config.py::test_quality_rule_validation`:
one test, five cases. Fixtures: a function that prepares something for tests; `conftest.py`
holds shared ones.

## Local Spark (20 min)

Read `tests/conftest.py`: a session-scoped `spark` fixture (`local[2]`, UTC, temp
warehouse) and a `FakeDbutils`. Then write, together, a test for `deduplicate`:

```python
from pyspark.sql import Row
from common_utils.scd import deduplicate

def test_deduplicate_keeps_latest(spark):
    df = spark.createDataFrame([Row(id=1, v="old", ts=1), Row(id=1, v="new", ts=2)])
    rows = {r["id"]: r["v"] for r in deduplicate(df, ["id"], "ts").collect()}
    assert rows == {1: "new"}
```

Students then write a test for `row_hash` (null vs empty string hash differently) and
compare with `tests/test_transforms.py`. Point out the schema-string form
`spark.createDataFrame([...], "a string, b string")` to avoid `CANNOT_DETERMINE_TYPE`.

## Contract tests (15 min)

Read `tests/test_contracts.py` test by test and say what mistake each prevents:
Silver reading a Bronze table that does not exist; Gold SQL referencing an unknown
object or a literal catalog name; PII tags lost between layers; a source without a job
task; a credential committed; notebooks that forgot the bootstrap block. These run in
seconds and need no data — the best return on investment in the suite.

## End-to-end and integration (15 min)

`tests/test_end_to_end_local.py` runs Silver `prepare` on tiny synthetic CSVs in
`tests/fixtures/` (shaped like the real sources, no real names), builds every Gold SQL on
local Spark, then asserts PK uniqueness, zero FK orphans and the reconciliation from
Class 15. Show the fixture generator idea: one re-emitted order, one broken JSON, one
customer with two versions, one exact duplicate — every finding from Class 8 in five rows.

Delta `MERGE` cannot run locally (no Delta jars in CI), so
`tests/test_integration_databricks.py` is marked `integration`, deselected by default
(`pyproject.toml`), and run inside a Databricks notebook.

## Homework

1. Write a test that `render_sql` replaces all three placeholders.
2. Add a fixture row that violates `net_amount_not_negative` and make the end-to-end test fail; then remove it.
3. Run the integration tests in Databricks (instructions in the file docstring) and paste the output.

## Common problems

* `ModuleNotFoundError` in tests → `conftest.py` inserts the repo root in `sys.path`; run pytest from the root.
* Java not installed locally → local Spark needs a JDK 17.
* Flaky tests from leftover warehouse folders → the fixture uses a temp directory per session.
