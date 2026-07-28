from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from .config import Settings


class JsonFormatter(logging.Formatter):
    CONTEXT_FIELDS = (
        "consumer",
        "worker",
        "event_id",
        "event_type",
        "order_id",
        "aggregate_id",
        "reservation_request_id",
        "supplier_id",
        "correlation_id",
        "topic",
        "attempt",
        "attempt_count",
        "result",
        "rejection_reason",
        "duration_ms",
        "error_code",
    )

    def __init__(self, *, service: str, environment: str) -> None:
        super().__init__()
        self._service = service
        self._environment = environment

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self._service,
            "environment": self._environment,
            "message": record.getMessage(),
        }
        for field_name in self.CONTEXT_FIELDS:
            value = getattr(record, field_name, None)
            if value is not None:
                payload[field_name] = value
        if record.exc_info:
            payload["exception_type"] = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(settings: Settings) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(
        JsonFormatter(
            service=settings.service_name,
            environment=settings.environment,
        )
    )
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
