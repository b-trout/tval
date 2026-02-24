"""Structured JSON logging for tval.

Provides a JSON formatter that outputs log records as single-line JSON objects
with extra fields from the log record.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

_RESERVED = {
    "timestamp",
    "level",
    "module",
    "message",
    "name",
    "msg",
    "args",
    "created",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "exc_info",
    "exc_text",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    """Log formatter that outputs records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string with timestamp, level, module, and extras."""
        log: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED:
                log[key] = value
        if record.exc_info:
            log["exception"] = self.formatException(record.exc_info)
        return json.dumps(log, ensure_ascii=False)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with JSON formatting."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger
