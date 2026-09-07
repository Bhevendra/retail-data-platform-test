"""Contracts between the pieces of the project.

These need no Spark and run in milliseconds, and they catch the mistakes that are
most annoying to find at runtime: a config that does not match its notebook, a
quality rule on a table nobody builds, a credential committed by accident.
"""

import json
import re

import pytest
import yaml

SOURCE_CONFIG_KEYS = {"source_name", "source_type", "connection", "secret_keys", "landing_format", "bronze_table"}
SILVER_TABLES = {"customers", "sales_orders", "sales_order_lines", "sales_order_clicks", "sales"}


@pytest.fixture(scope="module")
def ingestion_configs(project_root):
    return {p.stem: json.loads(p.read_text()) for p in sorted((project_root / "src" / "ingestion" / "config").glob("*.json"))}


@pytest.fixture(scope="module")
def job(project_root):
    return yaml.safe_load((project_root / "resources" / "jobs.yml").read_text())["resources"]["jobs"]["retail_data_platform"]


# --------------------------------------------------------------------------- #
# Ingestion configs
# --------------------------------------------------------------------------- #
def test_every_source_config_is_complete(ingestion_configs):
    assert set(ingestion_configs) == {"sqlserver_customers", "cosmos_sales_orders", "s3_sales"}
    for name, config in ingestion_configs.items():
        missing = SOURCE_CONFIG_KEYS - set(config)
        assert not missing, f"{name}.json is missing {missing}"
        assert config["source_name"] == name, "the filename is the source name"
        assert config["landing_format"] in {"csv", "json", "parquet"}


def test_source_configs_hold_secret_names_not_secret_values(ingestion_configs):
    for name, config in ingestion_configs.items():
        for key, value in config["secret_keys"].items():
            assert re.fullmatch(r"[a-z0-9-]+", value), f"{name}.{key} should be a secret-scope key name, not a value"


# --------------------------------------------------------------------------- #
# Job wiring
# --------------------------------------------------------------------------- #
def test_every_source_has_an_ingestion_task(job, ingestion_configs):
    configured = set()
    for task in job["tasks"]:
        path = task["notebook_task"]["base_parameters"].get("config_path", "") if task["notebook_task"].get("base_parameters") else ""
        if path.startswith("src/ingestion/config/"):
            configured.add(path.split("/")[-1].removesuffix(".json"))
    assert configured == set(ingestion_configs), "a source with no task never runs"


def test_every_task_points_at_a_notebook_that_exists(job, project_root):
    for task in job["tasks"]:
        notebook = (project_root / "resources" / task["notebook_task"]["notebook_path"]).resolve()
        assert notebook.is_file(), f"task {task['task_key']} points at a missing notebook: {notebook}"
        assert notebook.suffix == ".ipynb"


def test_the_dag_is_wired_in_the_right_order(job):
    depends = {t["task_key"]: {d["task_key"] for d in t.get("depends_on", [])} for t in job["tasks"]}

    assert depends["bronze"] == {"ingest_sqlserver", "ingest_cosmos", "ingest_s3"}
    assert all(depends[t] == {"bronze"} for t in ["silver_customers", "silver_sales_orders", "silver_sales"])
    assert depends["gold"] == {"silver_customers", "silver_sales_orders", "silver_sales"}
    assert depends["quality"] == {"gold"} and depends["governance"] == {"gold"}
    for task in job["tasks"]:
        if task["task_key"] in {"bronze", "gold"}:
            assert task.get("run_if") == "ALL_SUCCESS", "a partial day must never load silently"


def test_run_date_is_a_job_parameter(job):
    parameters = {p["name"]: p["default"] for p in job["parameters"]}
    assert parameters["run_date"] == "{{job.start_time.iso_date}}", "one date for the whole run, not date.today() per task"
    assert {"catalog", "secret_scope", "bronze_schema", "silver_schema", "gold_schema"} <= set(parameters)


# --------------------------------------------------------------------------- #
# Quality and governance configs
# --------------------------------------------------------------------------- #
def test_quality_rules_reference_tables_the_pipeline_builds(project_root):
    config = json.loads((project_root / "quality" / "config" / "rules.json").read_text())
    bronze_tables = {json.loads(p.read_text())["bronze_table"] for p in (project_root / "src" / "ingestion" / "config").glob("*.json")}
    gold_tables = {re.sub(r"^\d+_|\.optional|\.sql$", "", p.name) for p in (project_root / "src" / "gold" / "sql").glob("*.sql")}
    known = {"bronze": bronze_tables, "silver": SILVER_TABLES, "gold": gold_tables}

    for check in config["checks"]:
        assert check["table"] in known[check["layer"]], f"quality rule on unknown table {check['layer']}.{check['table']}"
        for rule in check["rules"]:
            assert rule.get("severity", "error") in {"error", "warn"}
            assert rule["type"] in {"not_null", "unique", "accepted_values", "regex", "range", "min_row_count", "expression"}


def test_reconciliations_only_use_placeholders(project_root):
    config = json.loads((project_root / "quality" / "config" / "rules.json").read_text())
    assert config["reconciliations"], "the cross-table promises are the point of the quality run"
    for check in config["reconciliations"]:
        assert "${catalog}" in check["sql"] and "retaildataplatform." not in check["sql"]
        assert "passed" in check["sql"], "a reconciliation returns a single boolean column named 'passed'"


def test_governance_covers_every_gold_object(project_root):
    config = json.loads((project_root / "governance" / "config" / "tables.json").read_text())
    governed = {(t["schema"], t["name"]) for t in config["tables"]}
    gold_objects = {("gold", re.sub(r"^\d+_|\.optional|\.sql$", "", p.name)) for p in (project_root / "src" / "gold" / "sql").glob("*.sql")}
    ungoverned = gold_objects - governed - {("gold", "mv_web_sales"), ("gold", "mv_pos_sales")}
    assert not ungoverned, f"gold objects with no comments or keys: {ungoverned}"


def test_pii_columns_stay_declared_all_the_way_to_gold(project_root):
    """A PII column that loses its tag between layers is a compliance incident, not a typo."""
    config = json.loads((project_root / "governance" / "config" / "tables.json").read_text())
    by_name = {(t["schema"], t["name"]): set(t.get("pii_columns", [])) for t in config["tables"]}

    assert "customer_name" in by_name[("silver", "customers")]
    assert by_name[("silver", "customers")] <= by_name[("gold", "dim_customer")], "Gold must not lose Silver's PII tags"
    assert "customer_name" in by_name[("gold", "sales_order_lines_obt")], "views expose PII too"


def test_facts_declare_their_keys(project_root):
    config = json.loads((project_root / "governance" / "config" / "tables.json").read_text())
    for table in config["tables"]:
        if table["name"].startswith(("fact_", "dim_")):
            assert table.get("primary_key"), f"{table['name']} has no primary key — BI tools need it"
        if table["name"].startswith("fact_"):
            assert table.get("foreign_keys"), f"{table['name']} has no foreign keys — Power BI cannot build relationships"


# --------------------------------------------------------------------------- #
# Hygiene
# --------------------------------------------------------------------------- #
def test_no_credentials_are_committed(project_root):
    pattern = re.compile(r"(AKIA[0-9A-Z]{16}|mongodb(\+srv)?://[^<\s]+:[^<\s]+@|password\s*=\s*['\"][^'\"{$]+['\"])", re.IGNORECASE)
    folders = ["common_utils", "src", "quality", "governance", "resources"]
    for folder in folders:
        for path in (project_root / folder).rglob("*"):
            if path.is_file() and path.suffix in {".py", ".json", ".yml", ".sql", ".ipynb"}:
                assert not pattern.search(path.read_text(errors="ignore")), f"credential-like literal in {path}"


def test_notebooks_bootstrap_the_library_and_use_widgets(project_root):
    notebooks = list(project_root.rglob("*/code/*.ipynb"))
    assert len(notebooks) == 10, "3 ingestion + 1 bronze + 3 silver + 1 gold + quality + governance"
    for notebook in notebooks:
        content = json.loads(notebook.read_text())
        code = "\n".join("".join(c["source"]) for c in content["cells"] if c["cell_type"] == "code")
        assert '(candidate / "common_utils").is_dir()' in code, f"{notebook.name} cannot import common_utils"
        assert "dbutils.widgets" in code, f"{notebook.name} has no parameters"
        assert all(not c.get("outputs") for c in content["cells"] if c["cell_type"] == "code"), f"{notebook.name} has committed outputs"


def test_common_utils_stays_generic(project_root):
    """The library must not learn this project's vocabulary — that is what keeps it reusable."""
    domain_words = ["sales_order", "dim_customer", "retaildataplatform", "loyalty", "promo_id", "cosmos_sales"]
    for path in (project_root / "common_utils").glob("*.py"):
        text = path.read_text().lower()
        found = [word for word in domain_words if word in text]
        assert not found, f"{path.name} mentions {found} — move that into a layer notebook"
