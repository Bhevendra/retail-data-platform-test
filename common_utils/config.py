"""Typed configuration models.

Configuration is the contract of this platform: every pipeline behaviour is
declared in ``src/config/*.json`` and validated here *before* any Spark work
starts, so a typo fails fast in CI or at notebook start rather than after an
hour of compute.

The models use only the standard library (no pydantic) so they import on any
Databricks runtime and in CI without extra dependencies.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SOURCE_TYPES = {"cosmos_mongodb", "s3", "sqlserver"}
RULE_TYPES = {
    "not_null",
    "unique",
    "accepted_values",
    "regex",
    "range",
    "min_row_count",
    "expression",
}
SEVERITIES = {"error", "warn"}
FAILURE_MODES = {"fail", "quarantine", "warn"}
GOLD_PRODUCT_TYPES = {"table", "view", "metric_view", "date_dimension"}


class ConfigError(ValueError):
    """Raised when a configuration file violates the platform contract."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ConfigError(message)


def _identifier(value: str, what: str) -> str:
    _require(isinstance(value, str) and bool(_IDENTIFIER.match(value)), f"{what} must be a SQL identifier, got {value!r}")
    return value


# --------------------------------------------------------------------------- #
# Shared models
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class QualityRule:
    name: str
    type: str
    severity: str = "error"
    column: str | None = None
    values: list[Any] = field(default_factory=list)
    pattern: str | None = None
    min: float | None = None
    max: float | None = None
    expression: str | None = None
    description: str = ""

    @classmethod
    def from_dict(cls, raw: dict, entity: str) -> QualityRule:
        _require("name" in raw and "type" in raw, f"{entity}: every quality rule needs 'name' and 'type'")
        _require(raw["type"] in RULE_TYPES, f"{entity}: unsupported rule type {raw['type']!r}")
        severity = raw.get("severity", "error")
        _require(severity in SEVERITIES, f"{entity}: rule {raw['name']} severity must be one of {sorted(SEVERITIES)}")
        needs_column = raw["type"] in {"not_null", "unique", "accepted_values", "regex", "range"}
        _require(not needs_column or raw.get("column"), f"{entity}: rule {raw['name']} requires 'column'")
        if raw["type"] == "accepted_values":
            _require(bool(raw.get("values")), f"{entity}: rule {raw['name']} requires non-empty 'values'")
        if raw["type"] == "regex":
            _require(bool(raw.get("pattern")), f"{entity}: rule {raw['name']} requires 'pattern'")
        if raw["type"] == "range":
            _require(raw.get("min") is not None or raw.get("max") is not None, f"{entity}: rule {raw['name']} requires 'min' and/or 'max'")
        if raw["type"] == "min_row_count":
            _require(isinstance(raw.get("min"), (int, float)), f"{entity}: rule {raw['name']} requires numeric 'min'")
        if raw["type"] == "expression":
            _require(bool(raw.get("expression")), f"{entity}: rule {raw['name']} requires a SQL 'expression' that is true for valid rows")
        return cls(
            name=raw["name"],
            type=raw["type"],
            severity=severity,
            column=raw.get("column"),
            values=list(raw.get("values", [])),
            pattern=raw.get("pattern"),
            min=raw.get("min"),
            max=raw.get("max"),
            expression=raw.get("expression"),
            description=raw.get("description", ""),
        )


@dataclass(frozen=True)
class Platform:
    catalog: str
    owner: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    ops_schema: str = "ops"
    grants: dict[str, list[str]] = field(default_factory=dict)  # privilege -> [principals]

    @classmethod
    def from_dict(cls, raw: dict) -> Platform:
        _require("catalog" in raw, "platform.catalog is required")
        return cls(
            catalog=_identifier(raw["catalog"], "platform.catalog"),
            owner=raw.get("owner"),
            tags={str(k): str(v) for k, v in raw.get("tags", {}).items()},
            ops_schema=_identifier(raw.get("ops_schema", "ops"), "platform.ops_schema"),
            grants={k: list(v) for k, v in raw.get("grants", {}).items()},
        )


# --------------------------------------------------------------------------- #
# Bronze
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Source:
    name: str
    type: str
    format: str
    target_table: str
    primary_keys: list[str]
    description: str = ""
    secret_keys: dict[str, str] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)  # type-specific connection settings
    pii_columns: list[str] = field(default_factory=list)
    column_comments: dict[str, str] = field(default_factory=dict)
    quality_rules: list[QualityRule] = field(default_factory=list)
    on_quality_failure: str = "fail"
    read_options: dict[str, str] = field(default_factory=dict)
    schema_hints: dict[str, str] = field(default_factory=dict)  # column -> spark type for landed CSV/JSON

    @classmethod
    def from_dict(cls, raw: dict) -> Source:
        name = raw.get("name", "<unnamed>")
        for key in ("name", "type", "format", "target_table", "primary_keys"):
            _require(key in raw, f"source {name}: '{key}' is required")
        _require(raw["type"] in SOURCE_TYPES, f"source {name}: unsupported type {raw['type']!r}")
        _require(bool(raw["primary_keys"]), f"source {name}: primary_keys must not be empty")
        mode = raw.get("on_quality_failure", "fail")
        _require(mode in FAILURE_MODES, f"source {name}: on_quality_failure must be one of {sorted(FAILURE_MODES)}")
        type_required = {
            "cosmos_mongodb": ["database", "collection"],
            "s3": ["path", "region"],
            "sqlserver": ["host", "database", "table"],
        }[raw["type"]]
        options = {k: raw[k] for k in raw if k in {*type_required, "port", "jdbc_options", "query"}}
        for key in type_required:
            _require(key in options, f"source {name}: '{key}' is required for type {raw['type']}")
        return cls(
            name=_identifier(raw["name"], f"source {name}.name"),
            type=raw["type"],
            format=raw["format"],
            target_table=_identifier(raw["target_table"], f"source {name}.target_table"),
            primary_keys=list(raw["primary_keys"]),
            description=raw.get("description", ""),
            secret_keys=dict(raw.get("secret_keys", {})),
            options=options,
            pii_columns=list(raw.get("pii_columns", [])),
            column_comments=dict(raw.get("column_comments", {})),
            quality_rules=[QualityRule.from_dict(r, name) for r in raw.get("quality_rules", [])],
            on_quality_failure=mode,
            read_options={str(k): str(v) for k, v in raw.get("read_options", {}).items()},
            schema_hints=dict(raw.get("schema_hints", {})),
        )


@dataclass(frozen=True)
class BronzeConfig:
    platform: Platform
    schema: str
    raw_volume: str
    quality_results_table: str
    sources: list[Source]

    @classmethod
    def from_dict(cls, raw: dict) -> BronzeConfig:
        _require("platform" in raw and "sources" in raw, "bronze config needs 'platform' and 'sources'")
        platform_raw = raw["platform"]
        sources = [Source.from_dict(s) for s in raw["sources"]]
        names = [s.name for s in sources]
        _require(len(names) == len(set(names)), f"duplicate source names: {names}")
        tables = [s.target_table for s in sources]
        _require(len(tables) == len(set(tables)), f"duplicate bronze target tables: {tables}")
        return cls(
            platform=Platform.from_dict(platform_raw),
            schema=_identifier(platform_raw.get("schema", "bronze"), "platform.schema"),
            raw_volume=_identifier(platform_raw.get("raw_volume", "raw_data"), "platform.raw_volume"),
            quality_results_table=_identifier(platform_raw.get("quality_results_table", "data_quality_results"), "platform.quality_results_table"),
            sources=sources,
        )

    def source(self, name: str) -> Source:
        for source in self.sources:
            if source.name == name:
                return source
        raise ConfigError(f"unknown source {name!r}; known: {[s.name for s in self.sources]}")


# --------------------------------------------------------------------------- #
# Silver
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Explode:
    """Turn one row with an array column into one row per element (nested data -> its own table)."""

    column: str
    alias: str = "element"
    position_column: str | None = None  # 1-based position of the element within the array
    keep_array: bool = False

    @classmethod
    def from_dict(cls, raw: dict) -> Explode:
        _require(bool(raw.get("column")), "transformations.explode needs 'column'")
        alias = raw.get("alias", "element")
        _identifier(alias, "transformations.explode.alias")
        position = raw.get("position_column")
        if position:
            _identifier(position, "transformations.explode.position_column")
        return cls(column=raw["column"], alias=alias, position_column=position, keep_array=bool(raw.get("keep_array", False)))


@dataclass(frozen=True)
class Transformations:
    rename: dict[str, str] = field(default_factory=dict)
    cast: dict[str, str] = field(default_factory=dict)
    trim: list[str] = field(default_factory=list)
    parse_json: dict[str, str] = field(default_factory=dict)  # column -> DDL schema
    explode: Explode | None = None  # runs after parse_json, before derived
    derived: dict[str, str] = field(default_factory=dict)  # column -> SQL expression
    drop: list[str] = field(default_factory=list)
    null_literals: list[str] = field(default_factory=lambda: ["", "NULL", "null", "N/A"])
    filter: str | None = None

    @classmethod
    def from_dict(cls, raw: dict) -> Transformations:
        return cls(
            rename=dict(raw.get("rename", {})),
            cast=dict(raw.get("cast", {})),
            trim=list(raw.get("trim", [])),
            parse_json=dict(raw.get("parse_json", {})),
            explode=Explode.from_dict(raw["explode"]) if raw.get("explode") else None,
            derived=dict(raw.get("derived", {})),
            drop=list(raw.get("drop", [])),
            null_literals=list(raw.get("null_literals", ["", "NULL", "null", "N/A"])),
            filter=raw.get("filter"),
        )


@dataclass(frozen=True)
class SilverEntity:
    source_table: str
    target_table: str
    primary_keys: list[str]
    scd_type: int
    description: str = ""
    order_by: str = "_ingested_at"
    transformations: Transformations = field(default_factory=Transformations)
    exclude_from_hash: list[str] = field(default_factory=list)
    cluster_by: list[str] = field(default_factory=list)
    column_comments: dict[str, str] = field(default_factory=dict)
    pii_columns: list[str] = field(default_factory=list)
    quality_rules: list[QualityRule] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict) -> SilverEntity:
        name = raw.get("target_table", "<unnamed>")
        for key in ("source_table", "target_table", "primary_keys", "scd_type"):
            _require(key in raw, f"silver entity {name}: '{key}' is required")
        _require(raw["scd_type"] in (1, 2), f"silver entity {name}: scd_type must be 1 or 2")
        _require(bool(raw["primary_keys"]), f"silver entity {name}: primary_keys must not be empty")
        return cls(
            source_table=_identifier(raw["source_table"], f"silver entity {name}.source_table"),
            target_table=_identifier(raw["target_table"], f"silver entity {name}.target_table"),
            primary_keys=list(raw["primary_keys"]),
            scd_type=int(raw["scd_type"]),
            description=raw.get("description", ""),
            order_by=raw.get("order_by", "_ingested_at"),
            transformations=Transformations.from_dict(raw.get("transformations", {})),
            exclude_from_hash=list(raw.get("exclude_from_hash", [])),
            cluster_by=list(raw.get("cluster_by", raw["primary_keys"])),
            column_comments=dict(raw.get("column_comments", {})),
            pii_columns=list(raw.get("pii_columns", [])),
            quality_rules=[QualityRule.from_dict(r, name) for r in raw.get("quality_rules", [])],
        )


@dataclass(frozen=True)
class SilverConfig:
    platform: Platform
    bronze_schema: str
    silver_schema: str
    entities: list[SilverEntity]

    @classmethod
    def from_dict(cls, raw: dict) -> SilverConfig:
        _require("platform" in raw and "entities" in raw, "silver config needs 'platform' and 'entities'")
        p = raw["platform"]
        entities = [SilverEntity.from_dict(e) for e in raw["entities"]]
        targets = [e.target_table for e in entities]
        _require(len(targets) == len(set(targets)), f"duplicate silver target tables: {targets}")
        return cls(
            platform=Platform.from_dict(p),
            bronze_schema=_identifier(p.get("bronze_schema", "bronze"), "platform.bronze_schema"),
            silver_schema=_identifier(p.get("silver_schema", "silver"), "platform.silver_schema"),
            entities=entities,
        )


# --------------------------------------------------------------------------- #
# Gold
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ForeignKey:
    columns: list[str]
    references: str  # table name within the gold schema
    referenced_columns: list[str]


@dataclass(frozen=True)
class GoldProduct:
    name: str
    type: str
    sql: str | None = None
    yaml: str | None = None  # metric view definition
    description: str = ""
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    cluster_by: list[str] = field(default_factory=list)
    column_comments: dict[str, str] = field(default_factory=dict)
    pii_columns: list[str] = field(default_factory=list)
    tags: dict[str, str] = field(default_factory=dict)
    start_date: str | None = None  # date_dimension only
    end_date: str | None = None
    depends_on: list[str] = field(default_factory=list)
    quality_rules: list[QualityRule] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict) -> GoldProduct:
        name = raw.get("name", "<unnamed>")
        _require("name" in raw and "type" in raw, f"gold product {name}: 'name' and 'type' are required")
        _require(raw["type"] in GOLD_PRODUCT_TYPES, f"gold product {name}: unsupported type {raw['type']!r}")
        if raw["type"] in {"table", "view"}:
            _require(bool(raw.get("sql")), f"gold product {name}: 'sql' is required for type {raw['type']}")
        if raw["type"] == "metric_view":
            _require(bool(raw.get("yaml")), f"gold product {name}: 'yaml' is required for metric views")
        if raw["type"] == "date_dimension":
            _require(bool(raw.get("start_date")) and bool(raw.get("end_date")), f"gold product {name}: date_dimension needs start_date and end_date")
        fks = []
        for fk in raw.get("foreign_keys", []):
            for key in ("columns", "references", "referenced_columns"):
                _require(key in fk, f"gold product {name}: foreign key needs '{key}'")
            _require(len(fk["columns"]) == len(fk["referenced_columns"]), f"gold product {name}: foreign key column counts differ")
            fks.append(ForeignKey(list(fk["columns"]), _identifier(fk["references"], "foreign_keys.references"), list(fk["referenced_columns"])))
        return cls(
            name=_identifier(raw["name"], f"gold product {name}.name"),
            type=raw["type"],
            sql=raw.get("sql"),
            yaml=raw.get("yaml"),
            description=raw.get("description", ""),
            primary_key=list(raw.get("primary_key", [])),
            foreign_keys=fks,
            cluster_by=list(raw.get("cluster_by", [])),
            column_comments=dict(raw.get("column_comments", {})),
            pii_columns=list(raw.get("pii_columns", [])),
            tags={str(k): str(v) for k, v in raw.get("tags", {}).items()},
            start_date=raw.get("start_date"),
            end_date=raw.get("end_date"),
            depends_on=list(raw.get("depends_on", [])),
            quality_rules=[QualityRule.from_dict(r, name) for r in raw.get("quality_rules", [])],
        )


@dataclass(frozen=True)
class GoldConfig:
    platform: Platform
    silver_schema: str
    gold_schema: str
    products: list[GoldProduct]

    @classmethod
    def from_dict(cls, raw: dict) -> GoldConfig:
        _require("platform" in raw and "products" in raw, "gold config needs 'platform' and 'products'")
        p = raw["platform"]
        products = [GoldProduct.from_dict(x) for x in raw["products"]]
        names = [x.name for x in products]
        _require(len(names) == len(set(names)), f"duplicate gold product names: {names}")
        known = set(names)
        for product in products:
            for dep in product.depends_on:
                _require(dep in known, f"gold product {product.name}: depends_on {dep!r} is not a product")
            for fk in product.foreign_keys:
                _require(fk.references in known, f"gold product {product.name}: foreign key references unknown product {fk.references!r}")
        return cls(
            platform=Platform.from_dict(p),
            silver_schema=_identifier(p.get("silver_schema", "silver"), "platform.silver_schema"),
            gold_schema=_identifier(p.get("gold_schema", "gold"), "platform.gold_schema"),
            products=products,
        )

    def ordered_products(self) -> list[GoldProduct]:
        """Topologically order products by depends_on and foreign keys (dims before facts)."""
        by_name = {p.name: p for p in self.products}
        resolved: list[GoldProduct] = []
        seen: set[str] = set()

        def visit(product: GoldProduct, stack: tuple[str, ...]) -> None:
            if product.name in seen:
                return
            _require(product.name not in stack, f"gold dependency cycle: {' -> '.join(stack + (product.name,))}")
            deps = list(product.depends_on) + [fk.references for fk in product.foreign_keys]
            for dep in deps:
                visit(by_name[dep], stack + (product.name,))
            seen.add(product.name)
            resolved.append(product)

        for product in self.products:
            visit(product, ())
        return resolved


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def load_json(config_path: str) -> dict:
    """Load a JSON config from an absolute path, the CWD, or the repository root."""
    candidates = [Path(config_path), Path.cwd() / config_path, repo_root() / config_path]
    for candidate in candidates:
        if candidate.is_file():
            with candidate.open(encoding="utf-8") as handle:
                return json.load(handle)
    raise FileNotFoundError(f"Configuration file was not found: {config_path} (tried {[str(c) for c in candidates]})")


def load_bronze_config(path: str = "src/config/bronze.json") -> BronzeConfig:
    return BronzeConfig.from_dict(load_json(path))


def load_silver_config(path: str = "src/config/silver.json") -> SilverConfig:
    return SilverConfig.from_dict(load_json(path))


def load_gold_config(path: str = "src/config/gold.json") -> GoldConfig:
    return GoldConfig.from_dict(load_json(path))


def runtime_value(name: str, default: str) -> str:
    """Allow an environment variable to override a configured default."""
    return os.getenv(name, default)


def qualified(catalog: str, schema: str, object_name: str) -> str:
    """Fully qualified, back-quoted Unity Catalog name."""
    return f"`{catalog}`.`{schema}`.`{object_name}`"
