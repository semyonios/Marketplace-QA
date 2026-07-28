import logging
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

logger = logging.getLogger(__name__)

ERROR_MESSAGES: dict[str, str] = {
    "validation_error": "Validation failed",
    "internal_error": "Internal server error",
    "user_not_found": "User not found",
    "user_conflict": "User with this email already exists",
    "product_not_found": "Product not found",
    "favorite_not_found": "Favorite item not found",
    "cart_item_not_found": "Cart item not found",
    "cart_is_empty": "Cart is empty and no order items were provided",
    "invalid_quantity": "Quantity must be greater than zero",
    "insufficient_stock": "Insufficient stock",
    "product_inactive": "Product is inactive",
    "product_archived": "Product is archived",
    "order_not_found": "Order not found",
    "order_already_cancelled": "Order is already cancelled",
    "customer_not_found": "Customer not found",
    "invalid_customer_id": "Customer identifier must be positive",
    "invalid_request": "Invalid request",
    "internal_access_forbidden": "Internal service access is forbidden",
    "cart_version_conflict": "Cart version changed",
    "dependency_unavailable": "Required projection data is unavailable",
}


def _error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
    details: dict | None = None,
    field_errors: list[dict[str, str]] | None = None,
) -> JSONResponse:
    raw_correlation_id = request.headers.get("X-Correlation-ID")
    try:
        correlation_id = str(uuid.UUID(raw_correlation_id)) if raw_correlation_id else str(uuid.uuid4())
    except ValueError:
        correlation_id = str(uuid.uuid4())
    if status_code == 400:
        category = "VALIDATION"
    elif status_code == 403:
        category = "FORBIDDEN"
    elif status_code == 404:
        category = "NOT_FOUND"
    elif status_code == 409:
        category = "CONFLICT"
    elif status_code >= 500:
        category = "DEPENDENCY" if code == "dependency_unavailable" else "INTERNAL"
    else:
        category = "BUSINESS"
    return JSONResponse(
        status_code=status_code,
        headers={"X-Correlation-ID": correlation_id},
        content={
            "error": {
                "code": code,
                "category": category,
                "message": message,
                "details": details or {},
                "field_errors": field_errors or [],
                "retryable": status_code >= 500,
                "correlation_id": correlation_id,
                "timestamp": datetime.now(timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            }
        },
    )


def _format_validation_error(exc: RequestValidationError | ValidationError) -> str:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", []) if part != "body")
        raw_message = error.get("msg", "Validation error")
        if "greater than 0" in raw_message:
            raw_message = ERROR_MESSAGES["invalid_quantity"]
        messages.append(f"{location}: {raw_message}" if location else raw_message)
    return "; ".join(messages) or ERROR_MESSAGES["validation_error"]


def _normalize_error(detail: object) -> tuple[str, str, dict]:
    if isinstance(detail, dict):
        code = str(detail.get("code", "business_error"))
        message = str(detail.get("message", ERROR_MESSAGES.get(code, "Business rule conflict")))
        details = detail.get("details", {})
        return code, message, details if isinstance(details, dict) else {}

    if isinstance(detail, str):
        return detail, ERROR_MESSAGES.get(detail, detail.replace("_", " ").capitalize()), {}

    return "business_error", "Business rule conflict", {}


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        code, message, details = _normalize_error(exc.detail)
        logger.warning("business error method=%s path=%s code=%s message=%s", request.method, request.url.path, code, message)
        return _error_response(
            request,
            status_code=exc.status_code,
            code=code,
            message=message,
            details=details,
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        detail = _format_validation_error(exc)
        logger.warning("validation error method=%s path=%s message=%s", request.method, request.url.path, detail)
        return _error_response(
            request,
            status_code=400,
            code="validation_error",
            message=detail,
        )

    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
        detail = _format_validation_error(exc)
        logger.warning("validation error method=%s path=%s message=%s", request.method, request.url.path, detail)
        return _error_response(
            request,
            status_code=400,
            code="validation_error",
            message=detail,
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error method=%s path=%s", request.method, request.url.path)
        return _error_response(
            request,
            status_code=500,
            code="internal_error",
            message=ERROR_MESSAGES["internal_error"],
        )
