import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import ValidationError

logger = logging.getLogger(__name__)



def _format_validation_error(exc: RequestValidationError | ValidationError) -> str:
    messages: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error.get("loc", []) if part != "body")
        message = error.get("msg", "Ошибка валидации")
        messages.append(f"{location}: {message}" if location else message)
    return "; ".join(messages) or "Ошибка валидации входных данных"



def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        logger.warning("HTTP error on %s %s: %s", request.method, request.url.path, exc.detail)
        error_type = "INTERNAL_ERROR" if exc.status_code >= 500 else "BUSINESS_ERROR"
        return JSONResponse(status_code=exc.status_code, content={"detail": str(exc.detail), "error_type": error_type})

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        detail = _format_validation_error(exc)
        logger.warning("Validation error on %s %s: %s", request.method, request.url.path, detail)
        return JSONResponse(status_code=422, content={"detail": detail, "error_type": "VALIDATION_ERROR"})

    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
        detail = _format_validation_error(exc)
        logger.warning("Validation error on %s %s: %s", request.method, request.url.path, detail)
        return JSONResponse(status_code=422, content={"detail": detail, "error_type": "VALIDATION_ERROR"})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": "Внутренняя ошибка сервиса", "error_type": "INTERNAL_ERROR"},
        )
