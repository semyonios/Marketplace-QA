from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request, Response
from sqlalchemy.orm import Session

from ..enums import BusinessStatus
from ..errors import ServiceError
from ..mappers.order_mapper import (
    map_customer_order_list_item,
    map_order_to_response,
    map_supplier_order_list_item,
)
from ..schemas import (
    CustomerOrderListResponse,
    OrderResponse,
    SupplierOrderListResponse,
)
from ..services.order_read import (
    OrderListFilters,
    get_customer_order,
    get_supplier_order,
    list_customer_orders,
    list_supplier_orders,
)
from .dependencies import (
    TestActor,
    get_db,
    require_customer_actor,
    require_supplier_actor,
)

ALLOWED_LIST_QUERY_PARAMS = {"page", "limit", "status", "created_from", "created_to"}


def get_order_list_filters(
    request: Request,
    page: Annotated[int, Query(ge=1)] = 1,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    statuses: Annotated[list[BusinessStatus] | None, Query(alias="status")] = None,
    created_from: Annotated[datetime | None, Query()] = None,
    created_to: Annotated[datetime | None, Query()] = None,
) -> OrderListFilters:
    unknown_params = sorted(set(request.query_params.keys()) - ALLOWED_LIST_QUERY_PARAMS)
    if unknown_params:
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message="Unknown query parameters",
            status_code=400,
            details={"parameters": unknown_params},
        )

    for field_name, value in (("created_from", created_from), ("created_to", created_to)):
        if value is not None and (value.tzinfo is None or value.utcoffset() != timedelta(0)):
            raise ServiceError(
                code="invalid_request",
                category="VALIDATION",
                message=f"{field_name} must be an UTC timestamp",
                status_code=400,
                field_errors=[{"field": field_name, "message": "UTC timestamp is required"}],
            )
    if created_from is not None and created_to is not None and created_from >= created_to:
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message="created_from must be earlier than created_to",
            status_code=400,
        )

    normalized_statuses = tuple(dict.fromkeys(statuses or []))
    return OrderListFilters(
        page=page,
        limit=limit,
        statuses=normalized_statuses,
        created_from=created_from,
        created_to=created_to,
    )


def create_order_read_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["Orders"])

    @router.get(
        "/customers/{customer_id}/orders",
        response_model=CustomerOrderListResponse,
    )
    def customer_orders(
        customer_id: Annotated[int, Path(gt=0)],
        filters: OrderListFilters = Depends(get_order_list_filters),
        actor: TestActor = Depends(require_customer_actor),
        db: Session = Depends(get_db),
    ) -> CustomerOrderListResponse:
        _require_path_subject(actor=actor, path_subject_id=customer_id)
        page = list_customer_orders(db, customer_id=customer_id, filters=filters)
        return CustomerOrderListResponse(
            items=[
                map_customer_order_list_item(order, items_count=items_count)
                for order, items_count in page.rows
            ],
            page=page.page,
            limit=page.limit,
            count=page.count,
            total=page.total,
        )

    @router.get(
        "/customers/{customer_id}/orders/{order_id}",
        response_model=OrderResponse,
    )
    def customer_order_detail(
        customer_id: Annotated[int, Path(gt=0)],
        order_id: uuid.UUID,
        response: Response,
        actor: TestActor = Depends(require_customer_actor),
        db: Session = Depends(get_db),
    ) -> OrderResponse:
        _require_path_subject(actor=actor, path_subject_id=customer_id)
        order = get_customer_order(db, customer_id=customer_id, order_id=order_id)
        if order is None:
            raise _order_not_found()
        response.headers["ETag"] = f'"{order.version}"'
        return map_order_to_response(order, actor_role="CUSTOMER")

    @router.get(
        "/suppliers/{supplier_id}/orders",
        response_model=SupplierOrderListResponse,
    )
    def supplier_orders(
        supplier_id: Annotated[int, Path(gt=0)],
        filters: OrderListFilters = Depends(get_order_list_filters),
        actor: TestActor = Depends(require_supplier_actor),
        db: Session = Depends(get_db),
    ) -> SupplierOrderListResponse:
        _require_path_subject(actor=actor, path_subject_id=supplier_id)
        page = list_supplier_orders(db, supplier_id=supplier_id, filters=filters)
        return SupplierOrderListResponse(
            items=[
                map_supplier_order_list_item(order, items_count=items_count)
                for order, items_count in page.rows
            ],
            page=page.page,
            limit=page.limit,
            count=page.count,
            total=page.total,
        )

    @router.get(
        "/suppliers/{supplier_id}/orders/{order_id}",
        response_model=OrderResponse,
    )
    def supplier_order_detail(
        supplier_id: Annotated[int, Path(gt=0)],
        order_id: uuid.UUID,
        response: Response,
        actor: TestActor = Depends(require_supplier_actor),
        db: Session = Depends(get_db),
    ) -> OrderResponse:
        _require_path_subject(actor=actor, path_subject_id=supplier_id)
        order = get_supplier_order(db, supplier_id=supplier_id, order_id=order_id)
        if order is None:
            raise _order_not_found()
        response.headers["ETag"] = f'"{order.version}"'
        return map_order_to_response(order, actor_role="SUPPLIER")

    return router


def _require_path_subject(*, actor: TestActor, path_subject_id: int) -> None:
    if actor.subject_id != path_subject_id:
        raise ServiceError(
            code="order_access_forbidden",
            category="FORBIDDEN",
            message="Test subject does not match the requested owner",
            status_code=403,
        )


def _order_not_found() -> ServiceError:
    return ServiceError(
        code="order_not_found",
        category="NOT_FOUND",
        message="Order was not found",
        status_code=404,
    )
