from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.enums import (
    BusinessStatus,
    OperationState,
    ReservationState,
    TargetTerminalStatus,
)
from app.models import Order, OrderInbox, OrderItem, OrderOutbox, OrderStatusHistory
from app.services.operation_timeout import OperationTimeoutConfig, OperationTimeoutService
from app.services.stock_result import StockResultService, parse_stock_result

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
CONSUMER = "order-service-stock-events-v1"


def _seed(
    factory,
    *,
    operation: OperationState,
    business: BusinessStatus = BusinessStatus.RESERVED,
    reservation: ReservationState = ReservationState.RESERVED,
):
    if (
        operation
        in {OperationState.CANCELLATION_PENDING, OperationState.REJECTION_PENDING}
        and reservation == ReservationState.RESERVED
    ):
        reservation = ReservationState.RELEASE_REQUESTED
    order_id = uuid.uuid4()
    reservation_id = uuid.uuid4() if reservation != ReservationState.REQUESTED else None
    reservation_request_id = uuid.uuid4()
    operation_id = uuid.uuid4()
    correlation_id = uuid.uuid4()
    now = NOW - timedelta(seconds=10)
    kwargs = {}
    if operation == OperationState.CONFIRMATION_PENDING:
        kwargs.update(
            finalization_request_id=operation_id,
            finalization_requested_at=now,
            finalization_deadline_at=NOW + timedelta(seconds=20),
            finalization_attempt_count=1,
            finalization_last_attempt_at=now,
        )
    elif operation in {
        OperationState.CANCELLATION_PENDING,
        OperationState.REJECTION_PENDING,
    }:
        kwargs.update(
            release_request_id=operation_id,
            release_requested_at=now,
            release_deadline_at=NOW + timedelta(seconds=20),
            release_attempt_count=1,
            release_last_attempt_at=now,
            target_terminal_status=(
                TargetTerminalStatus.CANCELLED
                if operation == OperationState.CANCELLATION_PENDING
                else TargetTerminalStatus.REJECTED
            ),
        )
    with factory.begin() as session:
        session.add(
            Order(
                id=order_id,
                customer_id=101,
                supplier_id=201,
                cart_id=uuid.uuid4(),
                cart_version=order_id.int % 2_000_000_000 + 1,
                business_status=business,
                operation_state=operation,
                reservation_state=reservation,
                total_amount=Decimal("20.00"),
                currency="RUB",
                version=3,
                correlation_id=correlation_id,
                reservation_request_id=reservation_request_id,
                reservation_id=reservation_id,
                created_at=now,
                updated_at=now,
                items=[
                    OrderItem(
                        id=uuid.uuid4(),
                        product_id=1001,
                        product_name_snapshot="Lifecycle",
                        quantity=2,
                        unit_price=Decimal("10.00"),
                        line_total=Decimal("20.00"),
                        currency="RUB",
                        supplier_id_snapshot=201,
                    )
                ],
                **kwargs,
            )
        )
    return {
        "order_id": order_id,
        "reservation_id": reservation_id,
        "reservation_request_id": reservation_request_id,
        "operation_id": operation_id,
        "correlation_id": correlation_id,
    }


def _command(seeded, event_type: str, *, event_id=None, reason_code=None):
    payload = {
        "order_id": str(seeded["order_id"]),
        "supplier_id": 201,
        "reservation_request_id": str(seeded["reservation_request_id"]),
        "reservation_id": (
            str(seeded["reservation_id"]) if seeded["reservation_id"] else None
        ),
    }
    if event_type in {"StockFinalized", "StockFinalizationFailed"}:
        payload["finalization_request_id"] = str(seeded["operation_id"])
        if event_type == "StockFinalized":
            payload.update(result="FINALIZED", finalized_at="2026-07-28T12:00:00.000Z")
        else:
            payload.update(
                reason_code=reason_code or "RESERVATION_NOT_ACTIVE",
                retryable=False,
                occurred_at="2026-07-28T12:00:00.000Z",
            )
    elif event_type in {"StockReleased", "StockReleaseFailed"}:
        payload["release_request_id"] = str(seeded["operation_id"])
        if event_type == "StockReleased":
            payload.update(result="RELEASED", released_at="2026-07-28T12:00:00.000Z")
        else:
            payload.update(
                reason_code=reason_code or "UNKNOWN_RESERVATION_STATE",
                retryable=False,
                occurred_at="2026-07-28T12:00:00.000Z",
            )
    elif event_type == "StockReservationSucceeded":
        payload.update(
            reservation_id=str(uuid.uuid4()),
            reserved_items=[{"product_id": 1001, "quantity": 2}],
            reserved_at="2026-07-28T12:00:00.000Z",
        )
    else:
        payload.update(
            failure_category="BUSINESS",
            reason_code="INSUFFICIENT_STOCK",
            failed_items=[
                {
                    "product_id": 1001,
                    "requested_quantity": 2,
                    "available_quantity": 0,
                }
            ],
            occurred_at="2026-07-28T12:00:00.000Z",
            retryable=False,
        )
    envelope = {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": "2026-07-28T12:00:00.000Z",
        "producer": "supplier-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(seeded["order_id"]),
        "correlation_id": str(seeded["correlation_id"]),
        "causation_id": str(uuid.uuid4()),
        "payload": payload,
    }
    headers = {
        "event_id": envelope["event_id"],
        "event_type": event_type,
        "event_version": "1",
        "correlation_id": envelope["correlation_id"],
        "causation_id": envelope["causation_id"],
        "producer": "supplier-service",
        "content_type": "application/json",
    }
    return parse_stock_result(
        envelope,
        headers=headers,
        key=str(seeded["order_id"]),
    )


def _service(factory):
    return StockResultService(
        session_factory=factory,
        consumer_name=CONSUMER,
        clock=lambda: NOW,
    )


@pytest.mark.integration
def test_finalization_result_confirms_order(stock_result_session_factory) -> None:
    seeded = _seed(
        stock_result_session_factory,
        operation=OperationState.CONFIRMATION_PENDING,
    )
    command = _command(seeded, "StockFinalized")

    first = _service(stock_result_session_factory).process(command)
    duplicate = _service(stock_result_session_factory).process(command)

    assert first.result == "CONFIRMED"
    assert duplicate.result == "DUPLICATE_EVENT"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        assert order.business_status == BusinessStatus.CONFIRMED
        assert order.operation_state == OperationState.NONE
        assert order.version == 4
        assert session.scalar(select(OrderOutbox.event_type)) == "OrderConfirmed"


@pytest.mark.integration
@pytest.mark.parametrize(
    ("operation", "terminal", "event_type"),
    [
        (
            OperationState.CANCELLATION_PENDING,
            BusinessStatus.CANCELLED,
            "OrderCancelled",
        ),
        (
            OperationState.REJECTION_PENDING,
            BusinessStatus.REJECTED,
            "OrderRejected",
        ),
    ],
)
def test_release_result_finishes_saved_terminal_target(
    stock_result_session_factory,
    operation,
    terminal,
    event_type,
) -> None:
    seeded = _seed(stock_result_session_factory, operation=operation)

    result = _service(stock_result_session_factory).process(
        _command(seeded, "StockReleased")
    )

    assert result.result == terminal.value
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        assert order.business_status == terminal
        assert order.operation_state == OperationState.NONE
        assert order.reservation_state == ReservationState.RELEASED
        assert session.scalar(select(OrderOutbox.event_type)) == event_type


@pytest.mark.integration
def test_late_reservation_success_creates_durable_compensation(
    stock_result_session_factory,
) -> None:
    seeded = _seed(
        stock_result_session_factory,
        operation=OperationState.CANCELLATION_PENDING,
        business=BusinessStatus.PENDING_RESERVATION,
        reservation=ReservationState.RELEASE_REQUESTED,
    )

    result = _service(stock_result_session_factory).process(
        _command(seeded, "StockReservationSucceeded")
    )

    assert result.result == "LATE_SUCCESS_RELEASE_REQUESTED"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        outbox = session.scalar(select(OrderOutbox))
        assert order.operation_state == OperationState.CANCELLATION_PENDING
        assert order.reservation_id is not None
        assert outbox.event_type == "StockReleaseRequested"
        assert outbox.payload["payload"]["reason"] == "COMPENSATION"
        assert outbox.payload["causation_id"] is not None


@pytest.mark.integration
def test_reservation_failure_after_cancel_finishes_cancelled(
    stock_result_session_factory,
) -> None:
    seeded = _seed(
        stock_result_session_factory,
        operation=OperationState.CANCELLATION_PENDING,
        business=BusinessStatus.PENDING_RESERVATION,
        reservation=ReservationState.RELEASE_REQUESTED,
    )

    result = _service(stock_result_session_factory).process(
        _command(seeded, "StockReservationFailed")
    )

    assert result.result == "CANCELLED_WITHOUT_RESERVATION"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        assert order.business_status == BusinessStatus.CANCELLED
        assert order.reservation_state == ReservationState.RELEASED


@pytest.mark.integration
def test_timeout_worker_retries_once_and_is_restart_safe(
    stock_result_session_factory,
) -> None:
    seeded = _seed(
        stock_result_session_factory,
        operation=OperationState.CONFIRMATION_PENDING,
    )
    worker = OperationTimeoutService(
        session_factory=stock_result_session_factory,
        config=OperationTimeoutConfig(batch_size=10, retry_seconds=5, max_attempts=5),
        clock=lambda: NOW,
    )

    assert worker.process_once() == 1
    assert worker.process_once() == 0
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        assert order.finalization_attempt_count == 2
        assert order.version == 4
        assert session.scalar(select(func.count()).select_from(OrderOutbox)) == 1
        assert session.scalar(select(func.count()).select_from(OrderStatusHistory)) == 1


@pytest.mark.integration
def test_timeout_worker_moves_exhausted_operation_to_failed(
    stock_result_session_factory,
) -> None:
    seeded = _seed(
        stock_result_session_factory,
        operation=OperationState.REJECTION_PENDING,
    )
    worker = OperationTimeoutService(
        session_factory=stock_result_session_factory,
        config=OperationTimeoutConfig(batch_size=10, retry_seconds=5, max_attempts=1),
        clock=lambda: NOW,
    )

    assert worker.process_once() == 1
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        assert order.operation_state == OperationState.FAILED
        assert order.failure_reason_code == "OPERATION_RETRY_EXHAUSTED"
        assert order.reservation_state == ReservationState.UNKNOWN
        assert session.scalar(select(OrderOutbox.event_type)) == "OrderProcessingFailed"
        assert session.scalar(select(func.count()).select_from(OrderInbox)) == 0


@pytest.mark.integration
def test_late_release_recovers_failed_release_to_saved_target(
    stock_result_session_factory,
) -> None:
    seeded = _seed(
        stock_result_session_factory,
        operation=OperationState.REJECTION_PENDING,
    )
    worker = OperationTimeoutService(
        session_factory=stock_result_session_factory,
        config=OperationTimeoutConfig(batch_size=10, retry_seconds=5, max_attempts=1),
        clock=lambda: NOW,
    )
    assert worker.process_once() == 1

    result = _service(stock_result_session_factory).process(
        _command(seeded, "StockReleased")
    )

    assert result.result == "REJECTED"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded["order_id"])
        assert order.business_status == BusinessStatus.REJECTED
        assert order.operation_state == OperationState.NONE
        assert order.reservation_state == ReservationState.RELEASED
        assert order.failure_reason_code is None
