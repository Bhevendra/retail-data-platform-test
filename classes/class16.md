# Class 16 — Modular programming: common_utils

The point where notebooks stop containing function definitions. Students learn modules,
packages, imports and typed configuration, then move every function they have written
into `common_utils/`.

## Objectives

* Explain module, package, `import`, `from … import …`, `sys.path` and `__init__.py`.
* Move functions into `common_utils/<topic>.py` and import them from a notebook.
* Explain why the notebook needs the "find the repo root" loop.
* Replace raw dictionaries with dataclasses that validate config on load.

## Time plan (100 min)

| Min | Segment |
| --- | --- |
| 0–10 | The pain: the same function pasted in four notebooks |
| 10–35 | Mini-lesson: modules and packages (toy example) |
| 35–60 | Migration: create `common_utils/`, move functions, fix imports |
| 60–85 | Mini-lesson: dataclasses and validation → `config.py` |
| 85–95 | Wire `load_bronze_config()` into `land_source` and `ds2b` |
| 95–100 | Homework |

## Mini-lesson: modules and packages (25 min)

Toy example in the workspace (Files, not notebooks):

```
bootcamp/
  mymath/
    __init__.py        (can be empty)
    sums.py            def sum2no(a, b): return a + b
  use_it.ipynb
```

```python
import sys
sys.path.insert(0, "/Workspace/Users/<you>/bootcamp")
from mymath.sums import sum2no
print(sum2no(3, 4))
```

Explain: a *module* is a `.py` file; a *package* is a folder with `__init__.py`; `import`
finds them by searching the folders in `sys.path`; the notebook's folder is not
automatically the repo root, so we search upwards:

```python
from pathlib import Path
for candidate in [Path.cwd(), *Path.cwd().parents]:
    if (candidate / "common_utils").is_dir():
        sys.path.insert(0, str(candidate))
        break
```

`Path.cwd().parents` is the list of parent folders; the loop stops at the first one that
contains `common_utils`. This exact block is at the top of every project notebook.

Two rules students must hear: a module must not call `spark` at import time (pass it as
a parameter), and `%pip` stays in the notebook, not in the module.

## Migration (25 min)

Create the files and move the functions written in Classes 5–15 into them. Do the first
one together, the rest in pairs:

| File | Functions from class |
| --- | --- |
| `common_utils/sources.py` | `read_sqlserver`, `read_cosmos`, `land_dataframe`, `land_s3_object`, `read_landed_raw`, `landing_path` (5, 6) |
| `common_utils/bronze.py` | `with_audit_columns`, `write_idempotent` (6, 7) |
| `common_utils/silver.py` | `apply_transformations`, `tolerant_cast`, `business_columns`, `prepare` (9, 12) |
| `common_utils/scd.py` | `row_hash`, `deduplicate`, `merge_type_1`, `merge_type_2` (10, 11) |
| `common_utils/gold.py` | `date_dimension`, `render_sql` (14, 15) |

Every function gets `spark` (and `dbutils` where used) as its first parameter — show
the diff on `read_landed_raw`. Then the notebook shrinks to imports + widgets + a loop.
Run `ds2b` from the module; same result, 40 lines instead of 200.

Show the reference `common_utils/__init__.py` docstring: a module map is documentation.

## Mini-lesson: dataclasses and validation (25 min)

Problem: a typo in `bronze.json` (`"primary_key"` instead of `"primary_keys"`) is found
40 minutes into a run. Solution: describe the expected shape once and check it on load.

Toy:

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Source:
    name: str
    type: str
    target_table: str
    primary_keys: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw):
        for key in ("name", "type", "target_table", "primary_keys"):
            if key not in raw:
                raise ValueError(f"source {raw.get('name', '?')}: '{key}' is required")
        if raw["type"] not in {"cosmos_mongodb", "s3", "sqlserver"}:
            raise ValueError(f"unsupported type {raw['type']}")
        return cls(raw["name"], raw["type"], raw["target_table"], list(raw["primary_keys"]))

s = Source.from_dict({"name": "x", "type": "s3", "target_table": "t", "primary_keys": ["id"]})
print(s.name, s.primary_keys)
s.name = "y"   # error: frozen
```

A class here is "a dictionary with a fixed set of named fields and a checker". `@dataclass`
writes the boilerplate; `frozen=True` prevents accidental edits; `from_dict` is the
validator. Open `common_utils/config.py` and read `Source.from_dict` — the class version
plus type-specific required keys and quality rules; then `BronzeConfig.from_dict` (duplicate
names check) and `load_bronze_config` (search paths). The point: **every mistake in a
config file is reported before any Spark job starts, with the entity name in the message.**

## Wire it in (10 min)

```python
from common_utils.config import load_bronze_config
config = load_bronze_config(dbutils.widgets.get("config_path"))
source = config.source(dbutils.widgets.get("source_name"))
print(source.type, source.options, source.secret_keys)
```

Notebook code changes from `source["type"]` to `source.type`. Break the JSON on purpose
(remove `"region"` from the S3 source) and watch the message.

## Homework

1. Move `merge_type_1/2` calls in `b2s` to use `common_utils.scd` and delete the notebook definitions.
2. Add a validation to `SilverEntity.from_dict`: `order_by` must be a string. Test it.
3. Explain in writing why `spark` is a parameter of every function in `common_utils` instead of a global.

## Common problems

* `ModuleNotFoundError: No module named 'common_utils'` — the repo root was not found (`Path.cwd()` is wrong when the notebook lives outside the repo) or the folder is misnamed. This is the exact error from the project; teach students to `print(sys.path[:3])`.
* Editing a module then rerunning the notebook uses the *old* code — use `%load_ext autoreload` / `%autoreload 2` in interactive sessions, or restart Python.
* Circular imports: `silver.py` imports `scd.py`, never the other way round. Draw the allowed direction: config ← runtime ← sources/quality/scd ← bronze/silver/gold.
