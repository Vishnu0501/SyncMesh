"""
logger.py
---------
Centralized structured logging for SyncMesh.

All modules import `get_logger` so every log record carries consistent
context (service name, hostname, timestamp).

Supports two output modes controlled by the LOG_JSON env variable:
  - LOG_JSON=false (default): human-readable coloured console output.
  - LOG_JSON=true           : JSON-encoded records for log aggregation
                              platforms (Loki, Cloud Logging, Datadog).
"""

from __future__ import annotations

import json
import logging
import socket
import sys
from datetime import datetime, timezone
from typing import Any

from config.settings import settings

_HOSTNAME = socket.gethostname()
_SERVICE = "syncmesh"

_COLOURS = {
    "DEBUG":    "\033[36m",    # Cyan
    "INFO":     "\033[32m",    # Green
    "WARNING":  "\033[33m",    # Yellow
    "ERROR":    "\033[31m",    # Red
    "CRITICAL": "\033[35m",    # Magenta
    "RESET":    "\033[0m",
}


# ---------------------------------------------------------------------------
# JSON formatter — for log aggregation pipelines
# ---------------------------------------------------------------------------
class _JsonFormatter(logging.Formatter):
    """Emits one JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level":     record.levelname,
            "logger":    record.name,
            "service":   _SERVICE,
            "host":      _HOSTNAME,
            "message":   record.getMessage(),
        }
        # Attach any extra fields passed via the `extra` kwarg
        for key, value in record.__dict__.items():
            if key not in logging.LogRecord.__dict__ and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


# ---------------------------------------------------------------------------
# Coloured console formatter — for local development
# ---------------------------------------------------------------------------
class _ConsoleFormatter(logging.Formatter):
    """Pretty-prints log records with level-based colour highlighting."""

    _FMT = (
        "%(colour)s%(levelname)-8s%(reset)s | "
        "%(asctime)s | %(name)-30s | %(message)s"
    )

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        record.colour = _COLOURS.get(record.levelname, "")  # type: ignore[attr-defined]
        record.reset  = _COLOURS["RESET"]                    # type: ignore[attr-defined]
        formatter = logging.Formatter(self._FMT, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


# ---------------------------------------------------------------------------
# Internal registry to avoid duplicate handlers on repeated calls
# ---------------------------------------------------------------------------
_configured_loggers: set[str] = set()


def get_logger(name: str) -> logging.Logger:
    """
    Return a fully configured logger for the given module name.

    Usage
    -----
    >>> from src.utils.logger import get_logger
    >>> log = get_logger(__name__)
    >>> log.info("Pipeline started", extra={"table": "customer_profiles"})
    """
    logger = logging.getLogger(name)

    if name in _configured_loggers:
        return logger

    logger.setLevel(settings.logging.level)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(settings.logging.level)
    handler.setFormatter(
        _JsonFormatter() if settings.logging.json_logs else _ConsoleFormatter()
    )

    logger.addHandler(handler)
    _configured_loggers.add(name)
    return logger
