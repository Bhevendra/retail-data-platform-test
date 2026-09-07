"""Run logging: one row per task per entity in ``<catalog>.ops.pipeline_runs``.

This has to be inline — only the task itself knows whether it succeeded, how many
rows it wrote and how long it took. Using it costs three lines in a notebook.
"""

from __future__ import annotations

import traceback
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from common_utils.logger import get_logger, log_error, log_info

RUNS_TABLE = "pipeline_runs"
RUNS_SCHEMA = (
    "run_id string, run_date date, task string, layer string, entity string, status string, "
    "rows_read long, rows_written long, started_at timestamp, finished_at timestamp, duration_seconds double, error_message string"
)

logger = get_logger("pipeline")


def new_run_id() -> str:
    return str(uuid.uuid4())


def ensure_ops_schema(spark, catalog: str, schema: str = "ops") -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT 'Operational metadata: pipeline runs and data-quality results'")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{RUNS_TABLE}` ({RUNS_SCHEMA})
        USING DELTA
        COMMENT 'One row per pipeline run, task and entity. Status is STARTED, SUCCEEDED or FAILED.'
        """
    )


class RunStats:
    """Mutable holder so a task can report row counts before the block exits."""

    def __init__(self) -> None:
        self.rows_read: int | None = None
        self.rows_written: int | None = None


@contextmanager
def track(spark, catalog: str, run_id: str, run_date: str, task: str, layer: str, entity: str, schema: str = "ops") -> Iterator[RunStats]:
    """Record SUCCEEDED/FAILED for one entity, then re-raise anything that went wrong."""
    started = datetime.now(timezone.utc)
    stats = RunStats()
    log_info(logger, "started", task=task, layer=layer, entity=entity, run_id=run_id, run_date=run_date)
    try:
        yield stats
    except Exception as exc:  # noqa: BLE001 - record, then re-raise
        detail = f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-1500:]}"
        _write(spark, catalog, schema, run_id, run_date, task, layer, entity, "FAILED", stats, started, detail)
        log_error(logger, "failed", task=task, layer=layer, entity=entity, run_id=run_id, error=str(exc))
        raise
    _write(spark, catalog, schema, run_id, run_date, task, layer, entity, "SUCCEEDED", stats, started, None)
    log_info(
        logger,
        "succeeded",
        task=task,
        layer=layer,
        entity=entity,
        run_id=run_id,
        rows_read=stats.rows_read,
        rows_written=stats.rows_written,
    )


def _write(spark, catalog, schema, run_id, run_date, task, layer, entity, status, stats, started, error) -> None:
    finished = datetime.now(timezone.utc)
    row = (
        run_id,
        datetime.strptime(run_date, "%Y-%m-%d").date(),
        task,
        layer,
        entity,
        status,
        stats.rows_read,
        stats.rows_written,
        started,
        finished,
        (finished - started).total_seconds(),
        error,
    )
    spark.createDataFrame([row], RUNS_SCHEMA).write.mode("append").saveAsTable(f"`{catalog}`.`{schema}`.`{RUNS_TABLE}`")
