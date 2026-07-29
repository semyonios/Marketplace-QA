from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from ..enums import (
    ActorType,
    BusinessStatus,
    OperationState,
    OutboxStatus,
    ReservationState,
    TargetTerminalStatus,
)
from ..errors import ServiceError
from ..models import Order, OrderOutbox, OrderStatusHistory

logger = logging.getLogger(__name__)

OPERATION_TIMEOUT = timedelta(seconds=30)
REJECT_REASONS = {"SUPPLIER_REJECTED", "QUALITY_ISSUE", "OTHER"}
CANCEL_REASONS = {"CUSTOMER_REQUEST", "DUPLICATE_ORDER", "OTHER"}


@dataclass(frozen=True, slots=True)
class OrderActionResult:
    order: Order
    status_code: int
    replayed: bool


def confirm_order(
    *,
    session: Session,
    supplier_id: int,
    order_id: uuid.UUID,
    expected_version: int,
    correlation_id: uuid.UUID,
) -> OrderActionResult:
    return _perform_action(
        session=session,
        owner_field="supplier_id",
        owner_id=supplier_id,
        order_id=order_id,
        expected_version=expected_version,
        correlation_id=correlation_id,
        action="CONFIRM",
        reason_code=None,
        reason_text=None,
    )


def reject_order(
    *,
    session: Session,
    supplier_id: int,
    order_id: uuid.UUID,
    expected_version: int,
    correlation_id: uuid.UUID,
    reason_code: str,
    reason_text: str | None,
) -> OrderActionResult:
    _validate_reason(reason_code, reason_text, REJECT_REASONS)
    return _perform_action(
        session=session,
        owner_field="supplier_id",
        owner_id=supplier_id,
        order_id=order_id,
        expected_version=expected_version,
        correlation_id=correlation_id,
        action="REJECT",
        reason_code=reason_code,
        reason_text=reason_text,
    )


def cancel_order(
    *,
    session: Session,
    customer_id: int,
    order_id: uuid.UUID,
    expected_version: int,
    correlation_id: uuid.UUID,
    reason_code: str,
    reason_text: str | None,
) -> OrderActionResult:
    _validate_reason(reason_code, reason_text, CANCEL_REASONS)
    return _perform_action(
        session=session,
        owner_field="customer_id",
        owner_id=customer_id,
        order_id=order_id,
        expected_version=expected_version,
        correlation_id=correlation_id,
        action="CANCEL",
        reason_code=reason_code,
        reason_text=reason_text,
    )


def _perform_action(
    *,
    session: Session,
    owner_field: Literal["customer_id", "supplier_id"],
    owner_id: int,
    order_id: uuid.UUID,
    expected_version: int,
    correlation_id: uuid.UUID,
    action: Literal["CONFIRM", "REJECT", "CANCEL"],
    reason_code: str | None,
    reason_text: str | None,
) -> OrderActionResult:
    now = datetime.now(timezone.utc)
    with session.begin():
        owner_column = getattr(Order, owner_field)
        order = session.scalar(
            select(Order)
            .options(selectinload(Order.items))
            .where(Order.id == order_id, owner_column == owner_id)
            .with_for_update()
        )
        if order is None:
            raise ServiceError(
                code="order_not_found",
                category="NOT_FOUND",
                message="Order was not found",
                status_code=404,
            )

        replay = _replay_result(
            order=order,
            action=action,
            reason_code=reason_code,
            reason_text=reason_text,
        )
        if replay:
            return OrderActionResult(order=order, status_code=200, replayed=True)

        if order.version != expected_version:
            raise ServiceError(
                code="order_version_conflict",
                category="CONFLICT",
                message="Order version does not match If-Match",
                status_code=412,
                details={
                    "current_version": order.version,
                    "current_status": order.status.value,
                },
                headers={"ETag": f'"{order.version}"'},
            )

        _validate_transition(order=order, action=action)
        version_before = order.version
        business_before = order.business_status
        operation_before = order.operation_state
        reservation_before = order.reservation_state
        operation_id = uuid.uuid4()

        if action == "CONFIRM":
            order.operation_state = OperationState.CONFIRMATION_PENDING
            order.finalization_request_id = operation_id
            order.finalization_requested_at = now
            order.finalization_deadline_at = now + OPERATION_TIMEOUT
            order.finalization_attempt_count = 1
            order.finalization_last_attempt_at = now
            trigger = "ConfirmOrder"
            domain_event = "OrderConfirmationRequested"
        else:
            order.operation_state = (
                OperationState.REJECTION_PENDING
                if action == "REJECT"
                else OperationState.CANCELLATION_PENDING
            )
            order.target_terminal_status = (
                TargetTerminalStatus.REJECTED
                if action == "REJECT"
                else TargetTerminalStatus.CANCELLED
            )
            order.reservation_state = ReservationState.RELEASE_REQUESTED
            order.release_request_id = operation_id
            order.release_requested_at = now
            order.release_deadline_at = now + OPERATION_TIMEOUT
            order.release_attempt_count = 1
            order.release_last_attempt_at = now
            if action == "REJECT":
                order.rejection_reason_code = reason_code
                order.rejection_reason_text = reason_text
                trigger = "RejectOrder"
                domain_event = "OrderRejectionRequested"
            else:
                order.cancellation_reason_code = reason_code
                order.cancellation_reason_text = reason_text
                trigger = "CancelOrder"
                domain_event = "OrderCancellationRequested"

        order.failure_phase = None
        order.failure_reason_code = None
        order.failure_reason_text = None
        order.version += 1
        order.updated_at = now
        history = OrderStatusHistory(
            id=uuid.uuid4(),
            order_id=order.id,
            business_status_before=business_before,
            business_status_after=order.business_status,
            operation_state_before=operation_before,
            operation_state_after=order.operation_state,
            reservation_state_before=reservation_before,
            reservation_state_after=order.reservation_state,
            trigger=trigger,
            actor_type=ActorType.SUPPLIER if action != "CANCEL" else ActorType.CUSTOMER,
            actor_id=owner_id,
            event_id=None,
            correlation_id=correlation_id,
            version_before=version_before,
            version_after=order.version,
            reason_code=reason_code,
            reason_text=reason_text,
            created_at=now,
        )
        domain_outbox = _order_event(
            order=order,
            event_type=domain_event,
            correlation_id=correlation_id,
            causation_id=None,
            occurred_at=now,
            reason_code=reason_code,
            reason_text=reason_text,
        )
        command_outbox = (
            _finalization_command(
                order=order,
                correlation_id=correlation_id,
                causation_id=domain_outbox.id,
                occurred_at=now,
            )
            if action == "CONFIRM"
            else _release_command(
                order=order,
                correlation_id=correlation_id,
                causation_id=domain_outbox.id,
                occurred_at=now,
                reason=(
                    "SUPPLIER_REJECTED"
                    if action == "REJECT"
                    else "CUSTOMER_CANCELLED"
                ),
            )
        )
        session.add_all([history, domain_outbox, command_outbox])

    logger.info(
        "order action accepted",
        extra={
            "order_id": str(order.id),
            "supplier_id": order.supplier_id,
            "correlation_id": str(correlation_id),
            "action": action.lower(),
            "state_before": operation_before.value,
            "state_after": order.operation_state.value,
            "version_before": version_before,
            "version_after": order.version,
            "result": "accepted",
        },
    )
    return OrderActionResult(order=order, status_code=202, replayed=False)


def _validate_transition(*, order: Order, action: str) -> None:
    if order.operation_state != OperationState.NONE:
        raise _state_conflict(order)
    if action in {"CONFIRM", "REJECT"}:
        allowed = order.business_status == BusinessStatus.RESERVED
    else:
        allowed = order.business_status in {
            BusinessStatus.PENDING_RESERVATION,
            BusinessStatus.RESERVED,
        }
    if not allowed:
        raise _state_conflict(order)


def _replay_result(
    *,
    order: Order,
    action: str,
    reason_code: str | None,
    reason_text: str | None,
) -> bool:
    if action == "CONFIRM":
        return (
            order.operation_state == OperationState.CONFIRMATION_PENDING
            or (
                order.operation_state == OperationState.NONE
                and order.business_status == BusinessStatus.CONFIRMED
            )
        )
    if action == "REJECT" and (
        order.operation_state == OperationState.REJECTION_PENDING
        or (
            order.operation_state == OperationState.NONE
            and order.business_status == BusinessStatus.REJECTED
        )
    ):
        if (
            order.rejection_reason_code == reason_code
            and order.rejection_reason_text == reason_text
        ):
            return True
        raise _state_conflict(order)
    if action == "CANCEL" and (
        order.operation_state == OperationState.CANCELLATION_PENDING
        or (
            order.operation_state == OperationState.NONE
            and order.business_status == BusinessStatus.CANCELLED
        )
    ):
        if (
            order.cancellation_reason_code == reason_code
            and order.cancellation_reason_text == reason_text
        ):
            return True
        raise _state_conflict(order)
    return False


def _validate_reason(
    reason_code: str,
    reason_text: str | None,
    allowed: set[str],
) -> None:
    if reason_code not in allowed or (reason_code == "OTHER" and not reason_text):
        raise ServiceError(
            code="invalid_request",
            category="VALIDATION",
            message="Order action reason is invalid",
            status_code=400,
        )


def _state_conflict(order: Order) -> ServiceError:
    return ServiceError(
        code="order_state_conflict",
        category="CONFLICT",
        message="Order action is not allowed in the current state",
        status_code=409,
        details={"current_status": order.status.value, "current_version": order.version},
        headers={"ETag": f'"{order.version}"'},
    )


def _order_event(
    *,
    order: Order,
    event_type: str,
    correlation_id: uuid.UUID,
    causation_id: uuid.UUID | None,
    occurred_at: datetime,
    reason_code: str | None = None,
    reason_text: str | None = None,
) -> OrderOutbox:
    payload: dict[str, Any] = {
        "order_id": str(order.id),
        "customer_id": order.customer_id,
        "supplier_id": order.supplier_id,
        "status": order.status.value,
        "business_status": order.business_status.value,
        "operation_state": order.operation_state.value,
        "reservation_state": order.reservation_state.value,
        "order_version": order.version,
    }
    if reason_code is not None:
        payload.update(reason_code=reason_code, reason_text=reason_text)
    return _outbox(
        order=order,
        event_type=event_type,
        correlation_id=correlation_id,
        causation_id=causation_id,
        occurred_at=occurred_at,
        business_payload=payload,
    )


def _finalization_command(
    *,
    order: Order,
    correlation_id: uuid.UUID,
    causation_id: uuid.UUID,
    occurred_at: datetime,
) -> OrderOutbox:
    return _outbox(
        order=order,
        event_type="StockFinalizationRequested",
        correlation_id=correlation_id,
        causation_id=causation_id,
        occurred_at=occurred_at,
        business_payload={
            "order_id": str(order.id),
            "supplier_id": order.supplier_id,
            "reservation_id": str(order.reservation_id),
            "reservation_request_id": str(order.reservation_request_id),
            "finalization_request_id": str(order.finalization_request_id),
            "order_version": order.version,
            "requested_at": _timestamp(occurred_at),
            "correlation_id": str(correlation_id),
        },
    )


def _release_command(
    *,
    order: Order,
    correlation_id: uuid.UUID,
    causation_id: uuid.UUID,
    occurred_at: datetime,
    reason: str,
) -> OrderOutbox:
    return _outbox(
        order=order,
        event_type="StockReleaseRequested",
        correlation_id=correlation_id,
        causation_id=causation_id,
        occurred_at=occurred_at,
        business_payload={
            "order_id": str(order.id),
            "supplier_id": order.supplier_id,
            "release_request_id": str(order.release_request_id),
            "reservation_id": (
                str(order.reservation_id) if order.reservation_id is not None else None
            ),
            "reservation_request_id": str(order.reservation_request_id),
            "reason": reason,
            "requested_at": _timestamp(occurred_at),
            "correlation_id": str(correlation_id),
        },
    )


def _outbox(
    *,
    order: Order,
    event_type: str,
    correlation_id: uuid.UUID,
    causation_id: uuid.UUID | None,
    occurred_at: datetime,
    business_payload: dict[str, Any],
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
        "correlation_id": str(correlation_id),
        "causation_id": str(causation_id) if causation_id else None,
        "payload": business_payload,
    }
    headers = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": "1",
        "correlation_id": str(correlation_id),
        "causation_id": str(causation_id) if causation_id else None,
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


def _timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
