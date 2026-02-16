from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any

from prep_brain.config import get_log_path, load_config

# Correlation ID for request tracing across service calls
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def get_correlation_id() -> str:
    """Get the current correlation ID, or generate one if not set."""
    cid = _correlation_id.get()
    if not cid:
        cid = uuid.uuid4().hex[:12]
        _correlation_id.set(cid)
    return cid


def set_correlation_id(cid: str) -> None:
    """Set the correlation ID for the current context."""
    _correlation_id.set(cid)


@contextmanager
def correlation_context(cid: str | None = None) -> Generator[str, None, None]:
    """Context manager for setting a correlation ID for a block of code."""
    old_cid = _correlation_id.get()
    new_cid = cid or uuid.uuid4().hex[:12]
    _correlation_id.set(new_cid)
    try:
        yield new_cid
    finally:
        _correlation_id.set(old_cid)


class CorrelationFilter(logging.Filter):
    """Logging filter that adds correlation_id to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = get_correlation_id() or "-"
        return True


class StructuredFormatter(logging.Formatter):
    """JSON structured log formatter for machine parsing."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "correlation_id": getattr(record, "correlation_id", "-"),
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_data"):
            log_entry["data"] = record.extra_data
        return json.dumps(log_entry)


def configure_logging(config: dict[str, Any] | None = None) -> None:
    cfg = config or load_config()
    level_name = str(cfg.get("logging", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    use_json = cfg.get("logging", {}).get("json_format", False)

    log_path = get_log_path(cfg)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(level)

    if root.handlers:
        # Avoid duplicate handlers when app is imported repeatedly in tests.
        return

    # Add correlation ID filter to all handlers
    correlation_filter = CorrelationFilter()

    if use_json:
        fmt = StructuredFormatter()
    else:
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(correlation_id)s | %(name)s | %(message)s"
        )

    stream = logging.StreamHandler()
    stream.setFormatter(fmt)
    stream.addFilter(correlation_filter)
    root.addHandler(stream)

    file_handler = RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    file_handler.addFilter(correlation_filter)
    root.addHandler(file_handler)


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **extra: Any,
) -> None:
    """Log a message with additional structured context data."""
    record = logger.makeRecord(
        logger.name,
        level,
        "(unknown)",
        0,
        message,
        (),
        None,
    )
    record.extra_data = extra
    logger.handle(record)
