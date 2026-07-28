from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from ..errors import ServiceError
from ..logging_config import correlation_id_context
from ..schemas import CreateOrderRequest, OrderResponse
from ..services.order_creation import create_order
from .dependencies import TestActor, get_db, require_customer_actor

logger = logging.getLogger(__name__)


def create_order_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["Orders"])

    @router.post(
        "/orders",
        response_model=OrderResponse,
        status_code=202,
        summary="Создать заказ",
        description="Создаёт single-supplier заказ по неизменяемому snapshot корзины. Повтор с тем же Idempotency-Key и телом возвращает существующий заказ.",
        responses={
            200: {
                "model": OrderResponse,
                "description": "Идемпотентный replay существующего заказа.",
                "headers": {
                    "ETag": {"description": "Текущая версия заказа", "schema": {"type": "string"}},
                    "Idempotency-Replayed": {"schema": {"type": "string"}},
                },
            },
            202: {
                "description": "Заказ принят для асинхронного резервирования.",
                "headers": {
                    "ETag": {"description": "Текущая версия заказа", "schema": {"type": "string"}},
                    "Location": {"schema": {"type": "string"}},
                    "Idempotency-Replayed": {"schema": {"type": "string"}},
                },
            },
            400: {"description": "Невалидный запрос или Idempotency-Key."},
            403: {"description": "Тестовый actor не владеет customer ID."},
            409: {"description": "Конфликт idempotency/cart snapshot/business rule."},
            503: {"description": "Customer snapshot service временно недоступен."},
        },
    )
    def create(
        order_request: CreateOrderRequest,
        request: Request,
        actor: TestActor = Depends(require_customer_actor),
        idempotency_key: str | None = Header(
            default=None,
            alias="Idempotency-Key",
            description="Обязательный ключ идемпотентности create order.",
            examples=["checkout-customer-1-cart-v7"],
        ),
        db: Session = Depends(get_db),
    ) -> JSONResponse:
        if actor.subject_id != order_request.customer_id:
            raise ServiceError(
                code="order_access_forbidden",
                category="FORBIDDEN",
                message="Test subject cannot create an order for another customer",
                status_code=403,
            )
        correlation_id = correlation_id_context.get()
        if correlation_id is None:
            raise ServiceError(
                code="internal_error",
                category="INTERNAL",
                message="Internal server error",
                status_code=500,
            )

        result = create_order(
            session=db,
            request=order_request,
            idempotency_key=idempotency_key,
            correlation_id=correlation_id,
            cart_client=request.app.state.cart_snapshot_client,
        )
        order_id = result.response.order_id
        logger.info(
            "create order completed order_id=%s customer_id=%s result_status=%s replayed=%s",
            order_id,
            order_request.customer_id,
            result.status_code,
            result.replayed,
        )
        return JSONResponse(
            status_code=result.status_code,
            content=result.response.model_dump(mode="json"),
            headers={
                "Location": f"/api/v1/customers/{order_request.customer_id}/orders/{order_id}",
                "ETag": f'"{result.response.version}"',
                "Idempotency-Replayed": str(result.replayed).lower(),
            },
        )

    return router
