import json
import os
from pathlib import Path


def load_config(config_path: str) -> dict:
    """Load JSON config from a DAB workspace file or local test checkout."""
    candidates = [Path(config_path)]
    candidates.append(Path.cwd() / config_path)
    # common_utils is at the repository root in the revised layout.
    candidates.append(Path(__file__).parents[1] / config_path)
    for candidate in candidates:
        if candidate.exists():
            with candidate.open() as handle:
                return json.load(handle)
    raise FileNotFoundError(f"Configuration file was not found: {config_path}")


def runtime_value(name: str, default: str) -> str:
    """Allow a DAB task parameter/environment value to safely override config."""
    return os.getenv(name, default)


def qualified(catalog: str, schema: str, object_name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{object_name}`"
