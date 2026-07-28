from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..config import Settings
from ..database import ReadinessCheckError, ReadinessState
from ..schemas import HealthResponse, ReadinessDependencies, ReadinessResponse

logger = logging.getLogger(__name__)


def create_router(
    *,
    settings: Settings,
    readiness_checker: Callable[[], ReadinessState],
) -> APIRouter:
    router = APIRouter(tags=["Operational"])

    @router.get(
        "/health",
        response_model=HealthResponse,
        summary="Проверить liveness",
        description="Проверяет, что HTTP-процесс order-service отвечает.",
    )
    def health() -> HealthResponse:
        return HealthResponse(status="ok", service=settings.service_name)

    @router.get(
        "/ready",
        response_model=ReadinessResponse,
        responses={503: {"model": ReadinessResponse}},
        summary="Проверить readiness",
        description="Проверяет PostgreSQL, Alembic revision и Kafka metadata.",
    )
    def ready(request: Request) -> ReadinessResponse | JSONResponse:
        checker: Callable[[], ReadinessState] = request.app.state.readiness_checker
        try:
            state = checker()
        except ReadinessCheckError as exc:
            logger.warning("readiness check failed reason=%s", str(exc))
            response = ReadinessResponse(
                status="not_ready",
                service=settings.service_name,
                dependencies=ReadinessDependencies(**asdict(exc.state)),
            )
            return JSONResponse(status_code=503, content=response.model_dump())

        return ReadinessResponse(
            status="ready",
            service=settings.service_name,
            dependencies=ReadinessDependencies(**asdict(state)),
        )

    return router
