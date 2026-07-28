from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request

from .api.router import create_router
from .config import Settings, get_settings
from .database import check_readiness, engine
from .errors import register_exception_handlers
from .logging_config import configure_logging, reset_correlation_id, set_correlation_id

logger = logging.getLogger(__name__)


def create_app(application_settings: Settings | None = None) -> FastAPI:
    settings = application_settings or get_settings()
    configure_logging(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        logger.info("application startup")
        try:
            yield
        finally:
            engine.dispose()
            logger.info("application shutdown")

    application = FastAPI(
        title="Marketplace-QA Order Service",
        description="Order lifecycle source of truth for Marketplace-QA 2.0.",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.state.readiness_checker = check_readiness

    @application.middleware("http")
    async def correlation_id_middleware(request: Request, call_next):
        raw_correlation_id = request.headers.get("X-Correlation-ID")
        try:
            correlation_id = str(uuid.UUID(raw_correlation_id)) if raw_correlation_id else str(uuid.uuid4())
        except ValueError:
            correlation_id = str(uuid.uuid4())

        token = set_correlation_id(correlation_id)
        try:
            response = await call_next(request)
            response.headers["X-Correlation-ID"] = correlation_id
            return response
        finally:
            reset_correlation_id(token)

    register_exception_handlers(application)
    application.include_router(create_router(settings=settings, readiness_checker=check_readiness))
    return application


app = create_app()


if __name__ == "__main__":
    runtime_settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=runtime_settings.host,
        port=runtime_settings.port,
        log_config=None,
    )
