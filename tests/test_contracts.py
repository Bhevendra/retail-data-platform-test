"""Cross-layer contracts: Silver must consume what Bronze produces, Gold what Silver produces, and no secrets in git."""

import re

import yaml

from common_utils.config import load_bronze_config, load_gold_config, load_silver_config, repo_root

ROOT = repo_root()


def test_silver_entities_map_to_bronze_tables():
    bronze_tables = {s.target_table for s in load_bronze_config().sources}
    for entity in load_silver_config().entities:
        assert entity.source_table in bronze_tables, f"silver entity {entity.target_table} reads unknown bronze table {entity.source_table}"


def test_gold_sql_only_references_silver_or_gold_objects():
    silver = load_silver_config()
    gold = load_gold_config()
    silver_tables = {e.target_table for e in silver.entities}
    gold_products = {p.name for p in gold.products}
    pattern = re.compile(r"\$\{catalog\}\.\$\{(silver|gold)\}\.(\w+)")
    for product in gold.products:
        for text in (product.sql or "", product.yaml or ""):
            for layer, table in pattern.findall(text):
                known = silver_tables if layer == "silver" else gold_products
                assert table in known, f"gold product {product.name} references unknown {layer} object {table}"
            # Hard-coded catalog/schema names defeat the environment parametrisation.
            assert "retaildataplatform." not in text, f"gold product {product.name}: use ${{catalog}} instead of a literal catalog name"


def test_pii_columns_declared_in_bronze_stay_declared_downstream():
    bronze = {s.target_table: set(s.pii_columns) for s in load_bronze_config().sources}
    for entity in load_silver_config().entities:
        renamed = entity.transformations.rename
        expected = {renamed.get(c, c) for c in bronze.get(entity.source_table, set())} - set(entity.transformations.drop)
        assert expected <= set(entity.pii_columns), f"silver {entity.target_table} lost PII tags for {expected - set(entity.pii_columns)}"


def test_job_tasks_match_configured_sources_and_notebooks():
    jobs = yaml.safe_load((ROOT / "resources" / "jobs.yml").read_text())["resources"]["jobs"]["retail_data_platform"]
    tasks = {t["task_key"]: t for t in jobs["tasks"]}
    landed = {t["notebook_task"]["base_parameters"]["source_name"] for t in tasks.values() if t["task_key"].startswith("land_")}
    assert landed == {s.name for s in load_bronze_config().sources}
    for task in tasks.values():
        notebook = (ROOT / "resources" / task["notebook_task"]["notebook_path"]).resolve()
        assert notebook.is_file(), f"task {task['task_key']} points at missing notebook {notebook}"
        config_path = task["notebook_task"]["base_parameters"].get("config_path")
        if config_path:
            assert (ROOT / config_path).is_file()
    assert set(tasks["ds2b"]["depends_on"][0].keys()) == {"task_key"}


def test_no_credential_literal_is_committed():
    prohibited = re.compile(r"(AKIA[0-9A-Z]{16}|mongodb(\+srv)?://[^<\s]+:[^<\s]+@|password\s*=\s*['\"][^'\"]+['\"])", re.IGNORECASE)
    files = list((ROOT / "src").rglob("*")) + list((ROOT / "common_utils").rglob("*.py")) + list((ROOT / "resources").rglob("*.yml")) + [ROOT / "databricks.yml"]
    for path in files:
        if path.is_file():
            assert not prohibited.search(path.read_text(errors="ignore")), f"credential-like literal in {path}"


def test_notebooks_are_valid_ipynb_and_bootstrap_the_library_path():
    import json

    notebooks = list((ROOT / "src").rglob("*.ipynb"))
    assert {n.name for n in notebooks} == {"land_source.ipynb", "ds2b.ipynb", "b2s.ipynb", "s2g.ipynb"}
    for notebook in notebooks:
        nb = json.loads(notebook.read_text())
        assert nb["nbformat"] == 4 and nb["cells"], notebook
        code = "\n".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
        assert '(candidate / "common_utils").is_dir()' in code, f"{notebook} does not bootstrap sys.path"
        assert "widget_context(" in code, f"{notebook} does not build a RunContext"
        assert all(not c.get("outputs") for c in nb["cells"] if c["cell_type"] == "code"), f"{notebook} has committed outputs"
