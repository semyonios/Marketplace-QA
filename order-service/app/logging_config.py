from __future__ import annotations

import json
import logging
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any

from .config import Settings

correlation_id_context: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def set_correlation_id(value: str) -> Token[str | None]:
    return correlation_id_context.set(value)


def reset_correlation_id(token: Token[str | None]) -> None:
    correlation_id_context.reset(token)


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, environment: str) -> None:
        super().__init__()
        self.service = service
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "service": self.service,
            "environment": self.environment,
            "message": record.getMessage(),
        }
        correlation_id = correlation_id_context.get()
        if correlation_id is not None:
            payload["correlation_id"] = correlation_id
        if record.exc_info:
            payload["exception_type"] = record.exc_info[0].__name__ if record.exc_info[0] else "Exception"
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(settings: Settings) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(settings.log_level)

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(settings.service_name, settings.environment))
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
