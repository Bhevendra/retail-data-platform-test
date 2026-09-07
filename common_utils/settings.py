"""Tiny helpers for reading JSON config files and secrets.

Deliberately small: this reads files and fetches secrets. It does not know the
*shape* of any config — that belongs to the project, not the library.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path


def project_root(marker: str = "common_utils") -> Path:
    """Walk up from the working directory until the folder containing ``marker`` is found."""
    for candidate in [Path.cwd(), *Path.cwd().parents]:
        if (candidate / marker).is_dir():
            return candidate
    raise RuntimeError(f"Could not find the project root (no '{marker}' folder above {Path.cwd()})")


def add_project_root_to_path(marker: str = "common_utils") -> str:
    """Make ``common_utils`` importable from a notebook wherever the notebook lives."""
    root = str(project_root(marker))
    if root not in sys.path:
        sys.path.insert(0, root)
    return root


def load_json(path: str) -> dict:
    """Load a JSON file relative to the project root, the working directory, or absolutely."""
    candidates = [Path(path), Path.cwd() / path]
    try:
        candidates.append(project_root() / path)
    except RuntimeError:
        pass
    for candidate in candidates:
        if candidate.is_file():
            return json.loads(candidate.read_text(encoding="utf-8"))
    raise FileNotFoundError(f"Config file not found: {path} (tried {[str(c) for c in candidates]})")


def load_json_folder(folder: str) -> dict[str, dict]:
    """Load every ``*.json`` in a folder, keyed by filename stem. Order is alphabetical."""
    base = Path(folder)
    if not base.is_dir():
        base = project_root() / folder
    return {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted(base.glob("*.json"))}


def get_secret(dbutils, scope: str, key: str) -> str:
    return dbutils.secrets.get(scope=scope, key=key)


def parse_run_date(value: str | None) -> str:
    """Accept an ISO date, an empty string, or an unresolved ``{{job...}}`` parameter."""
    if not value or not value.strip() or value.strip().startswith("{{"):
        return date.today().isoformat()
    return date.fromisoformat(value.strip()[:10]).isoformat()
