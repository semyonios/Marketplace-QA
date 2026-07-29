from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from functools import partial

import uvicorn
from fastapi import FastAPI, Request

from .api.order_read_router import create_order_read_router
from .api.order_action_router import create_order_action_router
from .api.order_router import create_order_router
from .api.router import create_router
from .clients.customer_service import CustomerServiceClient
from .config import Settings, get_settings
from .database import check_readiness, engine
from .errors import register_exception_handlers
from .logging_config import configure_logging, reset_correlation_id, set_correlation_id
from .messaging.kafka_producer import check_kafka_connectivity

logger = logging.getLogger(__name__)


def create_app(
    application_settings: Settings | None = None,
    *,
    cart_snapshot_client: CustomerServiceClient | None = None,
    kafka_readiness_checker: Callable[[], None] | None = None,
) -> FastAPI:
    settings = application_settings or get_settings()
    configure_logging(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        logger.info("application startup")
        try:
            yield
        finally:
            close_client = getattr(_app.state.cart_snapshot_client, "close", None)
            if callable(close_client):
                close_client()
            engine.dispose()
            logger.info("application shutdown")

    application = FastAPI(
        title="Marketplace-QA Order Service",
        description="Order lifecycle source of truth for Marketplace-QA 2.0.",
        version="0.1.0",
        lifespan=lifespan,
    )
    kafka_checker = kafka_readiness_checker or partial(
        check_kafka_connectivity,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        timeout_seconds=settings.readiness_timeout_seconds,
    )
    application.state.readiness_checker = partial(
        check_readiness,
        application_settings=settings,
        kafka_checker=kafka_checker,
    )
    application.state.cart_snapshot_client = cart_snapshot_client or CustomerServiceClient(
        base_url=settings.customer_service_url,
        timeout_seconds=settings.customer_service_timeout_seconds,
    )

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
    application.include_router(create_order_router())
    application.include_router(create_order_read_router())
    application.include_router(create_order_action_router())
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
