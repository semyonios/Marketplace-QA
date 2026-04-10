import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

logger = logging.getLogger(__name__)

ERROR_MESSAGES: dict[str, str] = {
    "validation_error": "Validation failed",
    "internal_error": "Internal server error",
    "supplier_not_found": "Supplier not found",
    "supplier_conflict": "Supplier with this email or phone already exists",
    "warehouse_not_found": "Warehouse not found",
    "product_not_found": "Product not found",
    "product_archived": "Product is archived",
}


def _format_validation_error(exc: RequestValidationError | ValidationError) -> str:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", []) if part != "body")
        message = error.get("msg", "Validation error")
        messages.append(f"{location}: {message}" if location else message)
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
