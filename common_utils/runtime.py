"""Run context, notebook parameters and structured logging.

A ``RunContext`` is created once per notebook run and threaded through every
layer so that all audit columns, quality results and operational metrics share
one ``run_id`` / ``run_date`` and can be joined together downstream.
"""

from __future__ import annotations

import json
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

LOGGER_NAME = "common_utils"


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - logging API
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        extra = getattr(record, "ctx", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    """Return a logger that emits one JSON object per line (easy to ship to a SIEM / log sink)."""
    logger = logging.getLogger(name)
    if not any(getattr(h, "_common_utils", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        handler._common_utils = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log(logger: logging.Logger, message: str, level: int = logging.INFO, **ctx: Any) -> None:
    logger.log(level, message, extra={"ctx": ctx})


def parse_run_date(value: str | None) -> date:
    """Accept ISO dates and the Databricks ``{{job.start_time.iso_date}}`` parameter."""
    if not value or value.strip() == "" or value.startswith("{{"):
        return date.today()
    return date.fromisoformat(value.strip()[:10])


@dataclass(frozen=True)
class RunContext:
    environment: str
    catalog: str
    secret_scope: str
    run_date: date
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task: str = "unknown"
    job_run_id: str | None = None

    @property
    def run_date_iso(self) -> str:
        return self.run_date.isoformat()

    def as_dict(self) -> dict[str, str]:
        return {
            "environment": self.environment,
            "catalog": self.catalog,
            "run_date": self.run_date_iso,
            "run_id": self.run_id,
            "task": self.task,
            "job_run_id": self.job_run_id or "",
        }


def widget_context(dbutils, task: str, defaults: dict[str, str]) -> RunContext:
    """Create widgets with defaults (interactive use) and build the RunContext from their values.

    In a job, DAB ``base_parameters`` populate the same widgets, so notebooks
    behave identically whether run manually or scheduled.
    """
    for name, default in defaults.items():
        dbutils.widgets.text(name, default)
    get = dbutils.widgets.get
    job_run_id = None
    try:  # Only available inside a job run.
        job_run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().get()
    except Exception:  # noqa: BLE001 - best effort metadata only
        job_run_id = None
    return RunContext(
        environment=get("environment"),
        catalog=get("catalog"),
        secret_scope=get("secret_scope"),
        run_date=parse_run_date(get("run_date")),
        task=task,
        job_run_id=job_run_id,
    )
