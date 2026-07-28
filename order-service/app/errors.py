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
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.category = category
        self.message = message
        self.status_code = status_code
        self.details = details or {}
        self.field_errors = field_errors or []
        self.retryable = retryable
        self.headers = headers or {}


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _error_response(error: ServiceError) -> JSONResponse:
    return JSONResponse(
        status_code=error.status_code,
        headers=error.headers,
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
    async def service_error_handler(request: Request, exc: ServiceError) -> JSONResponse:
        logger.warning(
            "controlled request error method=%s path=%s error_code=%s retryable=%s",
            request.method,
            request.url.path,
            exc.code,
            exc.retryable,
        )
        return _error_response(exc)

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
        locations = [error["loc"] for error in exc.errors()]
        if any("customer_id" in location for location in locations):
            code = "invalid_customer_id"
            message = "Customer identifier must be positive"
        elif any("supplier_id" in location for location in locations):
            code = "invalid_supplier_id"
            message = "Supplier identifier must be positive"
        elif any("order_id" in location for location in locations):
            code = "invalid_order_id"
            message = "Order identifier is invalid"
        else:
            code = "invalid_request"
            message = "Request validation failed"
        field_errors = [
            {
                "field": ".".join(str(part) for part in error["loc"] if part != "body"),
                "message": error["msg"],
            }
            for error in exc.errors()
        ]
        return _error_response(
            ServiceError(
                code=code,
                category="VALIDATION",
                message=message,
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
