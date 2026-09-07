"""JSON-line logging. One object per line so logs are searchable, not just readable."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any

DEFAULT_NAME = "pipeline"


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D102 - logging API
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            payload.update(context)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = DEFAULT_NAME) -> logging.Logger:
    """Return a logger that prints one JSON object per line (safe to call repeatedly)."""
    logger = logging.getLogger(name)
    if not any(getattr(handler, "_json_line", False) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        handler._json_line = True  # type: ignore[attr-defined]
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_info(logger: logging.Logger, message: str, **context: Any) -> None:
    logger.info(message, extra={"context": context})


def log_warning(logger: logging.Logger, message: str, **context: Any) -> None:
    logger.warning(message, extra={"context": context})


def log_error(logger: logging.Logger, message: str, **context: Any) -> None:
    logger.error(message, extra={"context": context})
