from __future__ import annotations

import re
import uuid
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Path
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from ..errors import ServiceError
from ..logging_config import correlation_id_context
from ..mappers.order_mapper import map_order_to_response
from ..schemas import (
    CancelOrderRequest,
    ConfirmOrderRequest,
    OrderActionReasonRequest,
    OrderResponse,
)
from ..services.order_actions import cancel_order, confirm_order, reject_order
from .dependencies import TestActor, get_db, require_customer_actor, require_supplier_actor

ETAG_PATTERN = re.compile(r'^"([1-9][0-9]*)"$')


def create_order_action_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["Orders"])

    @router.post(
        "/customers/{customer_id}/orders/{order_id}/cancel",
        response_model=OrderResponse,
        status_code=202,
        responses={200: {"model": OrderResponse}},
    )
    def customer_cancel(
        customer_id: Annotated[int, Path(gt=0)],
        order_id: uuid.UUID,
        body: CancelOrderRequest = Body(default_factory=CancelOrderRequest),
        actor: TestActor = Depends(require_customer_actor),
        if_match: str | None = Header(default=None, alias="If-Match"),
        db: Session = Depends(get_db),
    ) -> JSONResponse:
        _require_path_subject(actor, customer_id)
        correlation_id = _correlation_id()
        result = cancel_order(
            session=db,
            customer_id=customer_id,
            order_id=order_id,
            expected_version=_parse_if_match(if_match),
            correlation_id=correlation_id,
            reason_code=body.reason_code,
            reason_text=body.reason_text,
        )
        return _response(result, actor_role="CUSTOMER")

    @router.post(
        "/suppliers/{supplier_id}/orders/{order_id}/confirm",
        response_model=OrderResponse,
        status_code=202,
        responses={200: {"model": OrderResponse}},
    )
    def supplier_confirm(
        supplier_id: Annotated[int, Path(gt=0)],
        order_id: uuid.UUID,
        _body: ConfirmOrderRequest,
        actor: TestActor = Depends(require_supplier_actor),
        if_match: str | None = Header(default=None, alias="If-Match"),
        db: Session = Depends(get_db),
    ) -> JSONResponse:
        _require_path_subject(actor, supplier_id)
        result = confirm_order(
            session=db,
            supplier_id=supplier_id,
            order_id=order_id,
            expected_version=_parse_if_match(if_match),
            correlation_id=_correlation_id(),
        )
        return _response(result, actor_role="SUPPLIER")

    @router.post(
        "/suppliers/{supplier_id}/orders/{order_id}/reject",
        response_model=OrderResponse,
        status_code=202,
        responses={200: {"model": OrderResponse}},
    )
    def supplier_reject(
        supplier_id: Annotated[int, Path(gt=0)],
        order_id: uuid.UUID,
        body: OrderActionReasonRequest,
        actor: TestActor = Depends(require_supplier_actor),
        if_match: str | None = Header(default=None, alias="If-Match"),
        db: Session = Depends(get_db),
    ) -> JSONResponse:
        _require_path_subject(actor, supplier_id)
        result = reject_order(
            session=db,
            supplier_id=supplier_id,
            order_id=order_id,
            expected_version=_parse_if_match(if_match),
            correlation_id=_correlation_id(),
            reason_code=body.reason_code,
            reason_text=body.reason_text,
        )
        return _response(result, actor_role="SUPPLIER")

    return router


def _parse_if_match(value: str | None) -> int:
    if value is None:
        raise ServiceError(
            code="precondition_required",
            category="PRECONDITION",
            message="If-Match header is required",
            status_code=428,
        )
    match = ETAG_PATTERN.fullmatch(value)
    if match is None:
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message='If-Match must have the exact form "<positive version>"',
            status_code=400,
        )
    return int(match.group(1))


def _require_path_subject(actor: TestActor, path_subject_id: int) -> None:
    if actor.subject_id != path_subject_id:
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Test subject does not match the requested owner",
            status_code=403,
        )


def _correlation_id() -> uuid.UUID:
    value = correlation_id_context.get()
    if value is None:
        raise ServiceError(
            code="internal_error",
            category="INTERNAL",
            message="Internal server error",
            status_code=500,
        )
    return uuid.UUID(value)


def _response(result, *, actor_role: str) -> JSONResponse:
    representation = map_order_to_response(result.order, actor_role=actor_role)
    return JSONResponse(
        status_code=result.status_code,
        content=representation.model_dump(mode="json"),
        headers={
            "ETag": f'"{result.order.version}"',
            "Idempotency-Replayed": str(result.replayed).lower(),
        },
    )
