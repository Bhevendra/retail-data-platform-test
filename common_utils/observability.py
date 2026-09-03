"""Operational metadata: one row per (run, task, entity) in ``<catalog>.ops.pipeline_runs``.

Downstream teams and on-call engineers use this table to answer "did today's
load succeed, how many rows moved and how long did it take?" without opening
job logs. It is also the freshness signal BI dashboards should surface.
"""

from __future__ import annotations

import traceback
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from common_utils.config import qualified
from common_utils.runtime import RunContext, get_logger, log

PIPELINE_RUNS_TABLE = "pipeline_runs"
PIPELINE_RUNS_SCHEMA = (
    "run_id string, job_run_id string, environment string, task string, layer string, entity string, "
    "run_date date, status string, rows_read long, rows_written long, started_at timestamp, "
    "finished_at timestamp, duration_seconds double, error_message string"
)


def ensure_ops_schema(spark, catalog: str, ops_schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{ops_schema}` COMMENT 'Operational metadata: pipeline runs and data-quality results'")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified(catalog, ops_schema, PIPELINE_RUNS_TABLE)} ({PIPELINE_RUNS_SCHEMA})
        USING DELTA
        COMMENT 'One row per pipeline run, task and entity. Status is one of STARTED, SUCCEEDED, FAILED.'
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        """
    )


class EntityRun:
    """Mutable holder so the caller can report row counts before the context exits."""

    def __init__(self) -> None:
        self.rows_read: int | None = None
        self.rows_written: int | None = None


@contextmanager
def track_entity(spark, ctx: RunContext, catalog: str, ops_schema: str, layer: str, entity: str) -> Iterator[EntityRun]:
    """Record a SUCCEEDED/FAILED row for the entity and re-raise any failure."""
    logger = get_logger()
    started = datetime.now(timezone.utc)
    run = EntityRun()
    log(logger, "entity started", layer=layer, entity=entity, **ctx.as_dict())
    try:
        yield run
    except Exception as exc:  # noqa: BLE001 - we record then re-raise
        _write(spark, ctx, catalog, ops_schema, layer, entity, "FAILED", run, started, f"{type(exc).__name__}: {exc}\n{traceback.format_exc()[-2000:]}")
        log(logger, "entity failed", level=40, layer=layer, entity=entity, error=str(exc), **ctx.as_dict())
        raise
    _write(spark, ctx, catalog, ops_schema, layer, entity, "SUCCEEDED", run, started, None)
    log(logger, "entity succeeded", layer=layer, entity=entity, rows_read=run.rows_read, rows_written=run.rows_written, **ctx.as_dict())


def _write(spark, ctx: RunContext, catalog: str, ops_schema: str, layer: str, entity: str, status: str, run: EntityRun, started: datetime, error: str | None) -> None:
    finished = datetime.now(timezone.utc)
    row = (
        ctx.run_id,
        ctx.job_run_id,
        ctx.environment,
        ctx.task,
        layer,
        entity,
        ctx.run_date,
        status,
        run.rows_read,
        run.rows_written,
        started,
        finished,
        (finished - started).total_seconds(),
        error,
    )
    spark.createDataFrame([row], PIPELINE_RUNS_SCHEMA).write.mode("append").saveAsTable(qualified(catalog, ops_schema, PIPELINE_RUNS_TABLE))
