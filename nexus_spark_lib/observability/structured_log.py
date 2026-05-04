"""Structured logging for nexus_spark_lib.

Wraps nexus_core.logging to add spark-specific context (stage, partition_id,
batch_id). PII fields are NEVER logged — nexus_core's SensitiveFieldFilter
catches any stray log calls, and pii_flag=True on TransformedField adds a
second layer of protection.
"""

from __future__ import annotations

import logging
from typing import Any

from nexus_core.logging import get_logger as _core_get_logger


def get_stage_logger(module_name: str) -> logging.Logger:
    """Return a logger configured with nexus_core's SensitiveFieldFilter.

    Usage:
        logger = get_stage_logger(__name__)
        logger.info("Stage 1 complete: tenant=%s records=%d", tenant_id, count)
    """
    return _core_get_logger(module_name)


def log_pii_safe(
    logger: logging.Logger,
    level: int,
    message: str,
    fields: dict[str, Any],
    *,
    pii_flags: dict[str, bool] | None = None,
) -> None:
    """Log a structured message with PII fields redacted.

    Any field whose name appears in pii_flags with value True is replaced with
    "[REDACTED:pii]" before the log call.

    Args:
        logger:    The logger to use.
        level:     logging.INFO, logging.DEBUG, etc.
        message:   Log message template.
        fields:    Dict of key/value pairs to include.
        pii_flags: Map of field_name → bool (True = redact).
    """
    safe_fields: dict[str, Any] = {}
    for key, value in fields.items():
        if pii_flags and pii_flags.get(key, False):
            safe_fields[key] = "[REDACTED:pii]"
        else:
            safe_fields[key] = value
    logger.log(level, message, extra={"fields": safe_fields})
