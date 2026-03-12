"""Logging configuration for the Breach PII Search application.

Provides structured console logging with timestamps, log level, logger name,
and message. Designed for real-time monitoring of batch processing progress.

Usage:
    from app.logging_config import configure_logging
    configure_logging()

Or import LOG_FORMAT directly for testing:
    from app.logging_config import LOG_FORMAT
"""

import logging
import sys

# Spec-required log format: timestamp | level | logger | message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def configure_logging(level: int = logging.INFO) -> None:
    """Configure root logger for structured console (stdout) output.

    Sets up a StreamHandler writing to stdout with the LOG_FORMAT format
    and ISO-style timestamps. Safe to call multiple times — adds handler
    only if the root logger has no handlers yet.

    Args:
        level: Logging level to set on the root logger (default: INFO).
    """
    root_logger = logging.getLogger()

    if root_logger.handlers:
        # Already configured — don't add duplicate handlers
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(formatter)

    root_logger.setLevel(level)
    root_logger.addHandler(handler)
