"""Structured logging shared by producer and consumer (same pattern as Assignment 1)."""

import json
import logging
import os
import sys
from datetime import datetime, timezone

LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
os.makedirs(LOG_DIR, exist_ok=True)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    log_file = os.path.join(LOG_DIR, f"{name.replace('.', '_')}.log")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(JsonFormatter())

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s")
    )

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, message: str, **fields):
    logger.info(message, extra={"extra_fields": fields})
