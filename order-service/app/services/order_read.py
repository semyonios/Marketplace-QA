from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from ..enums import BusinessStatus
from ..models import Order, OrderItem


@dataclass(frozen=True, slots=True)
class OrderListFilters:
    page: int
    limit: int
    statuses: tuple[BusinessStatus, ...]
    created_from: datetime | None
    created_to: datetime | None


@dataclass(frozen=True, slots=True)
class OrderListPage:
    rows: tuple[tuple[Order, int], ...]
    page: int
    limit: int
    total: int

    @property
    def count(self) -> int:
        return len(self.rows)


def list_customer_orders(
    session: Session,
    *,
    customer_id: int,
    filters: OrderListFilters,
) -> OrderListPage:
    return _list_orders(
        session,
        owner_predicate=Order.customer_id == customer_id,
        filters=filters,
    )


def list_supplier_orders(
    session: Session,
    *,
    supplier_id: int,
    filters: OrderListFilters,
) -> OrderListPage:
    return _list_orders(
        session,
        owner_predicate=Order.supplier_id == supplier_id,
        filters=filters,
    )


def get_customer_order(
    session: Session,
    *,
    customer_id: int,
    order_id: uuid.UUID,
) -> Order | None:
    return session.scalar(
        select(Order)
        .options(selectinload(Order.items), selectinload(Order.history))
        .where(Order.id == order_id, Order.customer_id == customer_id)
    )


def get_supplier_order(
    session: Session,
    *,
    supplier_id: int,
    order_id: uuid.UUID,
) -> Order | None:
    return session.scalar(
        select(Order)
        .options(selectinload(Order.items), selectinload(Order.history))
        .where(Order.id == order_id, Order.supplier_id == supplier_id)
    )


def _list_orders(
    session: Session,
    *,
    owner_predicate,
    filters: OrderListFilters,
) -> OrderListPage:
    predicates = [owner_predicate]
    if filters.statuses:
        predicates.append(Order.business_status.in_(filters.statuses))
    if filters.created_from is not None:
        predicates.append(Order.created_at >= filters.created_from)
    if filters.created_to is not None:
        predicates.append(Order.created_at < filters.created_to)

    total = session.scalar(select(func.count(Order.id)).where(*predicates)) or 0
    items_count = (
        select(func.count(OrderItem.id))
        .where(OrderItem.order_id == Order.id)
        .correlate(Order)
        .scalar_subquery()
    )
    rows = session.execute(
        select(Order, items_count)
        .where(*predicates)
        .order_by(Order.created_at.desc(), Order.id.desc())
        .offset((filters.page - 1) * filters.limit)
        .limit(filters.limit)
    ).all()
    return OrderListPage(
        rows=tuple((order, int(count)) for order, count in rows),
        page=filters.page,
        limit=filters.limit,
        total=total,
    )
