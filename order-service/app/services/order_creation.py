from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from ..clients.customer_service import CustomerServiceClient
from ..enums import (
    ActorType,
    BusinessStatus,
    IdempotencyState,
    OperationState,
    OutboxStatus,
    ReservationState,
)
from ..errors import ServiceError
from ..models import Order, OrderIdempotency, OrderItem, OrderOutbox, OrderStatusHistory
from ..schemas import (
    AvailableActionsResponse,
    CartSnapshot,
    CreateOrderRequest,
    OrderItemResponse,
    OrderReasonResponse,
    OrderResponse,
)

logger = logging.getLogger(__name__)
MONEY_QUANT = Decimal("0.01")
IDEMPOTENCY_LEASE = timedelta(seconds=60)
IDEMPOTENCY_TTL = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class IdempotencyClaim:
    owner_token: uuid.UUID
    replay_order_id: uuid.UUID | None = None


@dataclass(frozen=True, slots=True)
class PreparedItem:
    product_id: int
    product_name: str
    supplier_id: int
    quantity: int
    unit_price: Decimal
    line_total: Decimal
    currency: str


@dataclass(frozen=True, slots=True)
class PreparedOrder:
    supplier_id: int
    items: tuple[PreparedItem, ...]
    total_amount: Decimal
    currency: str


@dataclass(frozen=True, slots=True)
class CreateOrderResult:
    response: OrderResponse
    status_code: int
    replayed: bool


def create_order(
    *,
    session: Session,
    request: CreateOrderRequest,
    idempotency_key: str | None,
    correlation_id: str,
    cart_client: CustomerServiceClient,
) -> CreateOrderResult:
    normalized_key = _validate_idempotency_key(idempotency_key)
    key_hash = hashlib.sha256(normalized_key.encode("ascii")).hexdigest()
    fingerprint = _request_fingerprint(request)
    claim = _claim_idempotency(
        session=session,
        customer_id=request.customer_id,
        key_hash=key_hash,
        fingerprint=fingerprint,
    )

    if claim.replay_order_id is not None:
        order = _load_order(session, claim.replay_order_id)
        return CreateOrderResult(response=serialize_order(order), status_code=200, replayed=True)

    existing_order = _find_order_by_cart_version(
        session,
        customer_id=request.customer_id,
        cart_version=request.cart_version,
    )
    if existing_order is not None:
        _complete_claim_with_existing_order(
            session=session,
            request=request,
            key_hash=key_hash,
            owner_token=claim.owner_token,
            order=existing_order,
        )
        order = _load_order(session, existing_order.id)
        return CreateOrderResult(response=serialize_order(order), status_code=200, replayed=True)

    try:
        snapshot = cart_client.get_cart_snapshot(
            customer_id=request.customer_id,
            expected_version=request.cart_version,
            correlation_id=correlation_id,
        )
        prepared = validate_and_prepare_snapshot(request=request, snapshot=snapshot)
        order = _persist_new_order(
            session=session,
            request=request,
            snapshot=snapshot,
            prepared=prepared,
            correlation_id=correlation_id,
            key_hash=key_hash,
            owner_token=claim.owner_token,
        )
    except IntegrityError as exc:
        session.rollback()
        winner = _find_order_by_cart_version(
            session,
            customer_id=request.customer_id,
            cart_version=request.cart_version,
        )
        if winner is not None:
            _complete_claim_with_existing_order(
                session=session,
                request=request,
                key_hash=key_hash,
                owner_token=claim.owner_token,
                order=winner,
            )
            order = _load_order(session, winner.id)
            return CreateOrderResult(response=serialize_order(order), status_code=200, replayed=True)
        _mark_claim_failed(
            session=session,
            request=request,
            key_hash=key_hash,
            owner_token=claim.owner_token,
            response_status=500,
            error_code="internal_error",
        )
        raise ServiceError(
            code="internal_error",
            category="INTERNAL",
            message="Internal server error",
            status_code=500,
        ) from exc
    except ServiceError as exc:
        session.rollback()
        if exc.retryable:
            _mark_claim_failed(
                session=session,
                request=request,
                key_hash=key_hash,
                owner_token=claim.owner_token,
                response_status=exc.status_code,
                error_code=exc.code,
            )
        else:
            _delete_claim(
                session=session,
                request=request,
                key_hash=key_hash,
                owner_token=claim.owner_token,
            )
        raise
    except Exception as exc:
        session.rollback()
        _mark_claim_failed(
            session=session,
            request=request,
            key_hash=key_hash,
            owner_token=claim.owner_token,
            response_status=500,
            error_code="internal_error",
        )
        logger.exception(
            "create order failed customer_id=%s cart_version=%s",
            request.customer_id,
            request.cart_version,
        )
        raise ServiceError(
            code="internal_error",
            category="INTERNAL",
            message="Internal server error",
            status_code=500,
        ) from exc

    return CreateOrderResult(response=serialize_order(order), status_code=202, replayed=False)


def _validate_idempotency_key(value: str | None) -> str:
    if value is None:
        raise ServiceError(
            code="idempotency_key_required",
            category="IDEMPOTENCY",
            message="Idempotency-Key header is required",
            status_code=400,
        )
    if not 1 <= len(value) <= 128 or any(ord(character) < 32 or ord(character) > 126 for character in value):
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message="Idempotency-Key must contain 1-128 printable ASCII characters",
            status_code=400,
        )
    return value


def _request_fingerprint(request: CreateOrderRequest) -> str:
    semantic_payload = {
        "operation": "CREATE_ORDER",
        "customer_id": request.customer_id,
        "cart_version": request.cart_version,
        "client_request_id": str(request.client_request_id) if request.client_request_id else None,
    }
    canonical = json.dumps(semantic_payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _claim_idempotency(
    *,
    session: Session,
    customer_id: int,
    key_hash: str,
    fingerprint: str,
) -> IdempotencyClaim:
    now = datetime.now(timezone.utc)
    owner_token = uuid.uuid4()
    record_id = uuid.uuid4()
    statement = (
        insert(OrderIdempotency)
        .values(
            id=record_id,
            customer_id=customer_id,
            operation_type="CREATE_ORDER",
            key_hash=key_hash,
            request_fingerprint=fingerprint,
            state=IdempotencyState.IN_PROGRESS,
            owner_token=owner_token,
            lease_expires_at=now + IDEMPOTENCY_LEASE,
        )
        .on_conflict_do_nothing(
            index_elements=["customer_id", "operation_type", "key_hash"],
        )
        .returning(OrderIdempotency.id)
    )
    created_id = session.scalar(statement)
    session.commit()
    if created_id is not None:
        return IdempotencyClaim(owner_token=owner_token)

    with session.begin():
        record = session.scalar(
            select(OrderIdempotency)
            .where(
                OrderIdempotency.customer_id == customer_id,
                OrderIdempotency.operation_type == "CREATE_ORDER",
                OrderIdempotency.key_hash == key_hash,
            )
            .with_for_update()
        )
        if record is None:
            raise ServiceError(
                code="internal_error",
                category="INTERNAL",
                message="Internal server error",
                status_code=500,
            )
        if record.request_fingerprint != fingerprint:
            raise ServiceError(
                code="idempotency_key_conflict",
                category="IDEMPOTENCY",
                message="Idempotency-Key was already used with another request",
                status_code=409,
            )
        if record.state == IdempotencyState.COMPLETED:
            if record.order_id is None:
                raise ServiceError(
                    code="internal_error",
                    category="INTERNAL",
                    message="Internal server error",
                    status_code=500,
                )
            return IdempotencyClaim(owner_token=owner_token, replay_order_id=record.order_id)

        lease_is_active = record.lease_expires_at is not None and record.lease_expires_at > now
        if record.state == IdempotencyState.IN_PROGRESS and lease_is_active:
            raise ServiceError(
                code="idempotency_request_in_progress",
                category="IDEMPOTENCY",
                message="The same request is already being processed",
                status_code=409,
                retryable=True,
                headers={"Retry-After": "1", "Idempotency-Replayed": "false"},
            )

        record.state = IdempotencyState.IN_PROGRESS
        record.owner_token = owner_token
        record.lease_expires_at = now + IDEMPOTENCY_LEASE
        record.response_status = None
        record.last_error_code = None
        record.updated_at = now
    return IdempotencyClaim(owner_token=owner_token)


def _find_order_by_cart_version(
    session: Session,
    *,
    customer_id: int,
    cart_version: int,
) -> Order | None:
    with session.begin():
        return session.scalar(
            select(Order).where(
                Order.customer_id == customer_id,
                Order.cart_version == cart_version,
            )
        )


def _load_order(session: Session, order_id: uuid.UUID) -> Order:
    order = session.scalar(
        select(Order)
        .options(selectinload(Order.items))
        .where(Order.id == order_id)
    )
    if order is None:
        raise ServiceError(
            code="internal_error",
            category="INTERNAL",
            message="Internal server error",
            status_code=500,
        )
    return order


def _complete_claim_with_existing_order(
    *,
    session: Session,
    request: CreateOrderRequest,
    key_hash: str,
    owner_token: uuid.UUID,
    order: Order,
) -> None:
    now = datetime.now(timezone.utc)
    with session.begin():
        record = _locked_claim(session, request.customer_id, key_hash)
        if record.state == IdempotencyState.COMPLETED and record.order_id == order.id:
            return
        if record.owner_token != owner_token:
            raise ServiceError(
                code="idempotency_request_in_progress",
                category="IDEMPOTENCY",
                message="The same request is already being processed",
                status_code=409,
                retryable=True,
                headers={"Retry-After": "1"},
            )
        record.state = IdempotencyState.COMPLETED
        record.order_id = order.id
        record.response_status = 200
        record.owner_token = None
        record.lease_expires_at = None
        record.last_error_code = None
        record.expires_at = now + IDEMPOTENCY_TTL
        record.updated_at = now


def validate_and_prepare_snapshot(
    *,
    request: CreateOrderRequest,
    snapshot: CartSnapshot,
) -> PreparedOrder:
    if snapshot.customer_id != request.customer_id:
        raise ServiceError(
            code="dependency_unavailable",
            category="DEPENDENCY",
            message="customer-service returned another customer snapshot",
            status_code=503,
            details={"dependency": "customer-service"},
            retryable=True,
        )
    if snapshot.cart_version != request.cart_version:
        raise ServiceError(
            code="cart_version_conflict",
            category="CONFLICT",
            message="Cart version changed",
            status_code=409,
            details={"current_version": snapshot.cart_version},
        )
    if not snapshot.items:
        raise ServiceError(
            code="cart_empty",
            category="BUSINESS",
            message="Cart is empty",
            status_code=409,
        )

    prepared_items: list[PreparedItem] = []
    product_ids: set[int] = set()
    supplier_ids: set[int] = set()
    currencies: set[str] = set()
    total_amount = Decimal("0.00")
    for index, item in enumerate(snapshot.items):
        if item.quantity <= 0:
            raise ServiceError(
                code="invalid_quantity",
                category="VALIDATION",
                message="Cart item quantity must be positive",
                status_code=400,
                field_errors=[
                    {
                        "field": f"items[{index}].quantity",
                        "message": "Value must be greater than zero",
                    }
                ],
            )
        if item.product_id <= 0 or item.supplier_id <= 0 or not item.product_name.strip():
            raise ServiceError(
                code="invalid_request",
                category="VALIDATION",
                message="Cart snapshot contains invalid product data",
                status_code=400,
            )
        if item.product_id in product_ids:
            raise ServiceError(
                code="invalid_request",
                category="VALIDATION",
                message="Cart snapshot contains duplicate products",
                status_code=400,
            )
        if item.product_status not in {"ACTIVE", "INACTIVE", "ARCHIVED"}:
            raise ServiceError(
                code="dependency_unavailable",
                category="DEPENDENCY",
                message="customer-service returned an unknown product status",
                status_code=503,
                details={"dependency": "customer-service"},
                retryable=True,
            )

        unit_price = item.unit_price.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
        if unit_price < 0:
            raise ServiceError(
                code="invalid_request",
                category="VALIDATION",
                message="Cart snapshot contains an invalid price",
                status_code=400,
            )
        currency = item.currency.upper()
        line_total = (unit_price * item.quantity).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
        total_amount += line_total
        product_ids.add(item.product_id)
        supplier_ids.add(item.supplier_id)
        currencies.add(currency)
        prepared_items.append(
            PreparedItem(
                product_id=item.product_id,
                product_name=item.product_name.strip(),
                supplier_id=item.supplier_id,
                quantity=item.quantity,
                unit_price=unit_price,
                line_total=line_total,
                currency=currency,
            )
        )

    if len(supplier_ids) != 1:
        raise ServiceError(
            code="cart_multiple_suppliers",
            category="BUSINESS",
            message="Cart contains products from multiple suppliers",
            status_code=409,
        )
    supplier_id = next(iter(supplier_ids))
    if snapshot.supplier_id != supplier_id:
        raise ServiceError(
            code="dependency_unavailable",
            category="DEPENDENCY",
            message="customer-service returned an inconsistent supplier",
            status_code=503,
            details={"dependency": "customer-service"},
            retryable=True,
        )
    if currencies != {"RUB"}:
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message="Cart snapshot must use RUB for every item",
            status_code=400,
        )
    total_amount = total_amount.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
    if total_amount <= 0:
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message="Order total must be greater than zero",
            status_code=400,
        )
    return PreparedOrder(
        supplier_id=supplier_id,
        items=tuple(prepared_items),
        total_amount=total_amount,
        currency="RUB",
    )


def _persist_new_order(
    *,
    session: Session,
    request: CreateOrderRequest,
    snapshot: CartSnapshot,
    prepared: PreparedOrder,
    correlation_id: str,
    key_hash: str,
    owner_token: uuid.UUID,
) -> Order:
    now = datetime.now(timezone.utc)
    order = Order(
        id=uuid.uuid4(),
        customer_id=request.customer_id,
        supplier_id=prepared.supplier_id,
        cart_id=snapshot.cart_id,
        cart_version=request.cart_version,
        business_status=BusinessStatus.PENDING_RESERVATION,
        operation_state=OperationState.NONE,
        reservation_state=ReservationState.REQUESTED,
        total_amount=prepared.total_amount,
        currency=prepared.currency,
        version=1,
        correlation_id=uuid.UUID(correlation_id),
        client_request_id=request.client_request_id,
        reservation_request_id=uuid.uuid4(),
        reservation_attempt_count=0,
        release_attempt_count=0,
        created_at=now,
        updated_at=now,
    )
    with session.begin():
        session.add(order)
        for item in prepared.items:
            session.add(
                OrderItem(
                    id=uuid.uuid4(),
                    order=order,
                    product_id=item.product_id,
                    product_name_snapshot=item.product_name,
                    quantity=item.quantity,
                    unit_price=item.unit_price,
                    line_total=item.line_total,
                    currency=item.currency,
                    supplier_id_snapshot=item.supplier_id,
                )
            )
        session.add(
            OrderStatusHistory(
                id=uuid.uuid4(),
                order=order,
                business_status_before=None,
                business_status_after=BusinessStatus.PENDING_RESERVATION,
                operation_state_before=None,
                operation_state_after=OperationState.NONE,
                reservation_state_before=None,
                reservation_state_after=ReservationState.REQUESTED,
                trigger="CREATE_ORDER",
                actor_type=ActorType.CUSTOMER,
                actor_id=request.customer_id,
                event_id=None,
                correlation_id=uuid.UUID(correlation_id),
                version_before=0,
                version_after=1,
                reason_code=None,
                reason_text=None,
                created_at=now,
            )
        )
        session.add_all(build_outbox_records(order=order, prepared=prepared, occurred_at=now))

        record = _locked_claim(session, request.customer_id, key_hash)
        if record.owner_token != owner_token or record.state != IdempotencyState.IN_PROGRESS:
            raise ServiceError(
                code="idempotency_request_in_progress",
                category="IDEMPOTENCY",
                message="The same request is already being processed",
                status_code=409,
                retryable=True,
                headers={"Retry-After": "1"},
            )
        record.state = IdempotencyState.COMPLETED
        record.order_id = order.id
        record.response_status = 202
        record.owner_token = None
        record.lease_expires_at = None
        record.last_error_code = None
        record.expires_at = now + IDEMPOTENCY_TTL
        record.updated_at = now
    return order


def build_outbox_records(
    *,
    order: Order,
    prepared: PreparedOrder,
    occurred_at: datetime,
) -> list[OrderOutbox]:
    order_created_payload = {
        "order_id": str(order.id),
        "customer_id": order.customer_id,
        "supplier_id": order.supplier_id,
        "cart_id": str(order.cart_id),
        "cart_version": order.cart_version,
        "status": "PENDING_RESERVATION",
        "total_amount": _money_string(order.total_amount),
        "currency": order.currency,
        "created_at": _timestamp(occurred_at),
    }
    stock_payload = {
        "order_id": str(order.id),
        "customer_id": order.customer_id,
        "supplier_id": order.supplier_id,
        "order_version": order.version,
        "reservation_request_id": str(order.reservation_request_id),
        "items": [
            {"product_id": item.product_id, "quantity": item.quantity}
            for item in prepared.items
        ],
        "reservation_deadline_at": None,
        "correlation_id": str(order.correlation_id),
    }
    return [
        _outbox_record(
            order=order,
            event_type="OrderCreated",
            business_payload=order_created_payload,
            occurred_at=occurred_at,
        ),
        _outbox_record(
            order=order,
            event_type="StockReservationRequested",
            business_payload=stock_payload,
            occurred_at=occurred_at,
        ),
    ]


def _outbox_record(
    *,
    order: Order,
    event_type: str,
    business_payload: dict,
    occurred_at: datetime,
) -> OrderOutbox:
    event_id = uuid.uuid4()
    envelope = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": _timestamp(occurred_at),
        "producer": "order-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order.id),
        "correlation_id": str(order.correlation_id),
        "causation_id": None,
        "payload": business_payload,
    }
    headers = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": "1",
        "correlation_id": str(order.correlation_id),
        "causation_id": None,
        "producer": "order-service",
        "content_type": "application/json",
    }
    return OrderOutbox(
        id=event_id,
        aggregate_type="ORDER",
        aggregate_id=order.id,
        event_type=event_type,
        event_version=1,
        payload=envelope,
        headers=headers,
        status=OutboxStatus.PENDING,
        attempt_count=0,
        next_attempt_at=occurred_at,
        created_at=occurred_at,
    )


def _locked_claim(session: Session, customer_id: int, key_hash: str) -> OrderIdempotency:
    record = session.scalar(
        select(OrderIdempotency)
        .where(
            OrderIdempotency.customer_id == customer_id,
            OrderIdempotency.operation_type == "CREATE_ORDER",
            OrderIdempotency.key_hash == key_hash,
        )
        .with_for_update()
    )
    if record is None:
        raise ServiceError(
            code="internal_error",
            category="INTERNAL",
            message="Internal server error",
            status_code=500,
        )
    return record


def _mark_claim_failed(
    *,
    session: Session,
    request: CreateOrderRequest,
    key_hash: str,
    owner_token: uuid.UUID,
    response_status: int,
    error_code: str,
) -> None:
    now = datetime.now(timezone.utc)
    with session.begin():
        record = _locked_claim(session, request.customer_id, key_hash)
        if record.state == IdempotencyState.COMPLETED or record.owner_token != owner_token:
            return
        record.state = IdempotencyState.FAILED_RETRYABLE
        record.response_status = response_status
        record.last_error_code = error_code
        record.owner_token = None
        record.lease_expires_at = None
        record.expires_at = now + IDEMPOTENCY_TTL
        record.updated_at = now


def _delete_claim(
    *,
    session: Session,
    request: CreateOrderRequest,
    key_hash: str,
    owner_token: uuid.UUID,
) -> None:
    with session.begin():
        record = _locked_claim(session, request.customer_id, key_hash)
        if record.state == IdempotencyState.IN_PROGRESS and record.owner_token == owner_token:
            session.delete(record)


def serialize_order(order: Order) -> OrderResponse:
    return OrderResponse(
        order_id=order.id,
        customer_id=order.customer_id,
        supplier_id=order.supplier_id,
        status=order.status.value,
        business_status=order.business_status.value,
        operation_state=order.operation_state.value,
        reservation_state=order.reservation_state.value,
        version=order.version,
        items=[
            OrderItemResponse(
                product_id=item.product_id,
                product_name=item.product_name_snapshot,
                quantity=item.quantity,
                unit_price=item.unit_price.quantize(MONEY_QUANT),
                line_total=item.line_total.quantize(MONEY_QUANT),
                currency=item.currency,
            )
            for item in sorted(order.items, key=lambda candidate: candidate.product_id)
        ],
        total_amount=order.total_amount.quantize(MONEY_QUANT),
        currency=order.currency,
        rejection_reason=(
            OrderReasonResponse(code=order.rejection_reason_code, text=order.rejection_reason_text)
            if order.rejection_reason_code
            else None
        ),
        cancellation_reason=(
            OrderReasonResponse(code=order.cancellation_reason_code, text=order.cancellation_reason_text)
            if order.cancellation_reason_code
            else None
        ),
        created_at=order.created_at,
        updated_at=order.updated_at,
        correlation_id=order.correlation_id,
        available_actions=AvailableActionsResponse(
            can_cancel=(
                order.business_status in {BusinessStatus.PENDING_RESERVATION, BusinessStatus.RESERVED}
                and order.operation_state == OperationState.NONE
            ),
            can_confirm=False,
            can_reject=False,
            can_retry=False,
            can_refresh=True,
        ),
    )


def _money_string(value: Decimal) -> str:
    return format(value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP), ".2f")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
