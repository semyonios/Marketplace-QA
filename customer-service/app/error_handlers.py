import logging

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
}


def _format_validation_error(exc: RequestValidationError | ValidationError) -> str:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", []) if part != "body")
        raw_message = error.get("msg", "Validation error")
        if "greater than 0" in raw_message:
            raw_message = ERROR_MESSAGES["invalid_quantity"]
        messages.append(f"{location}: {raw_message}" if location else raw_message)
    return "; ".join(messages) or ERROR_MESSAGES["validation_error"]


def _normalize_error(detail: object) -> tuple[str, str]:
    if isinstance(detail, dict):
        code = str(detail.get("code", "business_error"))
        message = str(detail.get("message", ERROR_MESSAGES.get(code, "Business rule conflict")))
        return code, message

    if isinstance(detail, str):
        return detail, ERROR_MESSAGES.get(detail, detail.replace("_", " ").capitalize())

    return "business_error", "Business rule conflict"


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        code, message = _normalize_error(exc.detail)
        logger.warning("business error method=%s path=%s code=%s message=%s", request.method, request.url.path, code, message)
        return JSONResponse(status_code=exc.status_code, content={"error": {"code": code, "message": message}})

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        detail = _format_validation_error(exc)
        logger.warning("validation error method=%s path=%s message=%s", request.method, request.url.path, detail)
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "validation_error", "message": detail}},
        )

    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
        detail = _format_validation_error(exc)
        logger.warning("validation error method=%s path=%s message=%s", request.method, request.url.path, detail)
        return JSONResponse(
            status_code=400,
            content={"error": {"code": "validation_error", "message": detail}},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error method=%s path=%s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"error": {"code": "internal_error", "message": ERROR_MESSAGES["internal_error"]}},
        )
