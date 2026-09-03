from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1]))
from common_utils.config import load_config, qualified


def test_bronze_config_has_required_source_contracts():
    config = load_config("src/bronze/config/bronze.json")
    assert {s["type"] for s in config["sources"]} == {"cosmos_mongodb", "s3", "sqlserver"}
    for source in config["sources"]:
        assert source["target_table"]
        assert source["primary_keys"]
        assert source["quality_rules"]


def test_qualified_name_is_uc_safe():
    assert qualified("retail", "bronze", "sales") == "`retail`.`bronze`.`sales`"
