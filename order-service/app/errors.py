from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .logging_config import correlation_id_context

logger = logging.getLogger(__name__)


class ServiceError(Exception):
    def __init__(
        self,
        *,
        code: str,
        category: str,
        message: str,
        status_code: int,
        details: dict[str, Any] | None = None,
        field_errors: list[dict[str, str]] | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.category = category
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        self.field_errors = field_errors or []
        self.retryable = retryable


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _error_response(error: ServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=error.status_code,
        content={
            "error": {
                "code": error.code,
                "category": error.category,
                "message": error.message,
                "details": error.details,
                "field_errors": error.field_errors,
                "retryable": error.retryable,
                "correlation_id": correlation_id_context.get(),
                "timestamp": _timestamp(),
            }
        },
    )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ServiceError)
    async def service_error_handler(_request: Request, exc: ServiceError) -> JSONResponse:
        return _error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        field_errors = [
            {
                "field": ".".join(str(part) for part in error["loc"] if part != "body"),
                "message": error["msg"],
            }
            for error in exc.errors()
        ]
        return _error_response(
            ServiceError(
                code="invalid_request",
                category="VALIDATION",
                message="Request validation failed",
                status_code=400,
                field_errors=field_errors,
            )
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled request error method=%s path=%s", request.method, request.url.path, exc_info=exc)
        return _error_response(
            ServiceError(
                code="internal_error",
                category="INTERNAL",
                message="Internal server error",
                status_code=500,
            )
        )
