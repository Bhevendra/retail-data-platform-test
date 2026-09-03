import json

import pytest

from retail_platform.config import (
    BronzeConfig,
    ConfigError,
    GoldConfig,
    QualityRule,
    SilverConfig,
    load_bronze_config,
    load_gold_config,
    load_silver_config,
    qualified,
)


def test_repository_configs_are_valid():
    bronze, silver, gold = load_bronze_config(), load_silver_config(), load_gold_config()
    assert {s.type for s in bronze.sources} == {"cosmos_mongodb", "s3", "sqlserver"}
    assert all(s.quality_rules for s in bronze.sources)
    assert silver.entities and gold.products


def test_qualified_name_is_uc_safe():
    assert qualified("retail", "bronze", "sales") == "`retail`.`bronze`.`sales`"


def test_bronze_rejects_unknown_source_type():
    raw = {"platform": {"catalog": "c"}, "sources": [{"name": "x", "type": "ftp", "format": "csv", "target_table": "t", "primary_keys": ["id"]}]}
    with pytest.raises(ConfigError, match="unsupported type"):
        BronzeConfig.from_dict(raw)


def test_bronze_rejects_missing_type_specific_keys():
    raw = {"platform": {"catalog": "c"}, "sources": [{"name": "x", "type": "s3", "format": "parquet", "target_table": "t", "primary_keys": ["id"], "path": "s3://b/k"}]}
    with pytest.raises(ConfigError, match="'region' is required"):
        BronzeConfig.from_dict(raw)


def test_bronze_rejects_duplicate_targets():
    src = {"name": "a", "type": "s3", "format": "parquet", "target_table": "t", "primary_keys": ["id"], "path": "s3://b/k", "region": "eu-north-1"}
    raw = {"platform": {"catalog": "c"}, "sources": [src, {**src, "name": "b"}]}
    with pytest.raises(ConfigError, match="duplicate bronze target tables"):
        BronzeConfig.from_dict(raw)


@pytest.mark.parametrize(
    "rule, message",
    [
        ({"name": "r", "type": "nope", "column": "c"}, "unsupported rule type"),
        ({"name": "r", "type": "not_null"}, "requires 'column'"),
        ({"name": "r", "type": "accepted_values", "column": "c"}, "non-empty 'values'"),
        ({"name": "r", "type": "range", "column": "c"}, "'min' and/or 'max'"),
        ({"name": "r", "type": "not_null", "column": "c", "severity": "fatal"}, "severity"),
    ],
)
def test_quality_rule_validation(rule, message):
    with pytest.raises(ConfigError, match=message):
        QualityRule.from_dict(rule, "entity")


def test_silver_rejects_bad_scd_type():
    raw = {"platform": {"catalog": "c"}, "entities": [{"source_table": "a", "target_table": "a", "primary_keys": ["id"], "scd_type": 3}]}
    with pytest.raises(ConfigError, match="scd_type"):
        SilverConfig.from_dict(raw)


def test_gold_orders_dimensions_before_facts_and_detects_cycles():
    raw = {
        "platform": {"catalog": "c"},
        "products": [
            {"name": "fact_sales", "type": "table", "sql": "select 1", "foreign_keys": [{"columns": ["customer_sk"], "references": "dim_customer", "referenced_columns": ["customer_sk"]}]},
            {"name": "dim_customer", "type": "table", "sql": "select 1"},
            {"name": "obt", "type": "view", "sql": "select 1", "depends_on": ["fact_sales"]},
        ],
    }
    ordered = [p.name for p in GoldConfig.from_dict(raw).ordered_products()]
    assert ordered.index("dim_customer") < ordered.index("fact_sales") < ordered.index("obt")

    cyclic = {"platform": {"catalog": "c"}, "products": [{"name": "a", "type": "view", "sql": "x", "depends_on": ["b"]}, {"name": "b", "type": "view", "sql": "x", "depends_on": ["a"]}]}
    with pytest.raises(ConfigError, match="cycle"):
        GoldConfig.from_dict(cyclic).ordered_products()


def test_gold_rejects_unknown_foreign_key_target():
    raw = {"platform": {"catalog": "c"}, "products": [{"name": "f", "type": "table", "sql": "x", "foreign_keys": [{"columns": ["k"], "references": "missing", "referenced_columns": ["k"]}]}]}
    with pytest.raises(ConfigError, match="unknown product"):
        GoldConfig.from_dict(raw)


def test_configs_are_valid_json_with_no_trailing_commas():
    from retail_platform.config import repo_root

    for name in ("bronze", "silver", "gold"):
        json.loads((repo_root() / "src" / "config" / f"{name}.json").read_text())
