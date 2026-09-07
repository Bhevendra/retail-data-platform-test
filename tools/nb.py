"""Convert Databricks-style .py sources into .ipynb notebooks (cells split on # COMMAND ----------)."""
import json, sys
from pathlib import Path

def convert(src: Path, delete_source: bool = True) -> Path:
    text = src.read_text()
    assert text.startswith("# Databricks notebook source"), src
    cells = []
    for chunk in text.split("\n", 1)[1].split("# COMMAND ----------"):
        chunk = chunk.strip("\n")
        if not chunk.strip():
            continue
        lines = chunk.split("\n")
        if all(l.startswith("# MAGIC") or not l.strip() for l in lines):
            magic = "\n".join(l[len("# MAGIC "):] if l.startswith("# MAGIC ") else "" for l in lines).strip("\n")
            if magic.startswith("%md"):
                cells.append({"cell_type": "markdown", "metadata": {}, "source": magic[3:].lstrip("\n").lstrip().splitlines(keepends=True)})
                continue
            chunk = magic
        cells.append({"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": chunk.splitlines(keepends=True)})
    nb = {
        "cells": cells,
        "metadata": {
            "application/vnd.databricks.v1+notebook": {
                "computePreferences": None, "dashboards": [], "environmentMetadata": {"base_environment": "", "environment_version": "2"},
                "language": "python", "notebookMetadata": {"pythonIndentUnit": 4}, "notebookName": src.stem, "widgets": {},
            },
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4, "nbformat_minor": 0,
    }
    dst = src.with_suffix(".ipynb")
    dst.write_text(json.dumps(nb, indent=1, ensure_ascii=False) + "\n")
    if delete_source:
        src.unlink()
    return dst

if __name__ == "__main__":
    for pattern in sys.argv[1:]:
        for p in sorted(Path(".").glob(pattern)):
            print("wrote", convert(p))
