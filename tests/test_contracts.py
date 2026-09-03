import json
from pathlib import Path


ROOT = Path(__file__).parents[1]


def test_configuration_contracts_are_complete():
    bronze = json.loads((ROOT / "src/bronze/config/bronze.json").read_text())
    silver = json.loads((ROOT / "src/silver/config/silver.json").read_text())
    bronze_tables = {source["target_table"] for source in bronze["sources"]}
    for entity in silver["entities"]:
        assert entity["source_table"] in bronze_tables
        assert entity["scd_type"] in {1, 2}
        assert entity["primary_keys"]


def test_no_credential_literal_is_committed():
    prohibited = ("AKIA", "mongodb://", "password =", "secret_access_key")
    files = list((ROOT / "src").rglob("*.json")) + list((ROOT / "src").rglob("*.py")) + list((ROOT / "common_utils").rglob("*.py"))
    contents = "\n".join(path.read_text().lower() for path in files)
    assert not any(token.lower() in contents for token in prohibited)
