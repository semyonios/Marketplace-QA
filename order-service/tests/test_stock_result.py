from __future__ import annotations

import copy
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.enums import (
    ActorType,
    BusinessStatus,
    InboxStatus,
    OperationState,
    OutboxStatus,
    ReservationState,
)
from app.models import (
    Order,
    OrderInbox,
    OrderItem,
    OrderOutbox,
    OrderStatusHistory,
)
from app.services.outbox_publisher import OutboxPublisher, OutboxPublisherConfig
from app.services.stock_result import (
    FAILURE_EVENT,
    SUCCESS_EVENT,
    IncompatibleStockResultError,
    StockResultService,
    parse_stock_result,
)

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
CONSUMER_NAME = "order-service-stock-events-v1"


@dataclass(frozen=True)
class SeededOrder:
    order_id: uuid.UUID
    reservation_request_id: uuid.UUID
    correlation_id: uuid.UUID
    supplier_id: int


class FakeProducer:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def publish(self, **kwargs) -> None:
        self.calls.append(copy.deepcopy(kwargs))

    def close(self) -> None:
        pass


def _seed_pending_order(factory) -> SeededOrder:
    seeded = SeededOrder(
        order_id=uuid.uuid4(),
        reservation_request_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        supplier_id=201,
    )
    with factory.begin() as session:
        order = Order(
            id=seeded.order_id,
            customer_id=101,
            supplier_id=seeded.supplier_id,
            cart_id=uuid.uuid4(),
            cart_version=7,
            business_status=BusinessStatus.PENDING_RESERVATION,
            operation_state=OperationState.NONE,
            reservation_state=ReservationState.REQUESTED,
            target_terminal_status=None,
            total_amount=Decimal("3500.99"),
            currency="RUB",
            version=1,
            correlation_id=seeded.correlation_id,
            reservation_request_id=seeded.reservation_request_id,
            reservation_requested_at=NOW,
            reservation_deadline_at=NOW + timedelta(seconds=30),
            reservation_attempt_count=1,
            reservation_last_attempt_at=NOW,
            release_attempt_count=0,
            created_at=NOW,
            updated_at=NOW,
        )
        session.add(order)
        session.add_all(
            [
                OrderItem(
                    id=uuid.uuid4(),
                    order_id=order.id,
                    product_id=1001,
                    product_name_snapshot="QA Laptop",
                    quantity=2,
                    unit_price=Decimal("1500.50"),
                    line_total=Decimal("3001.00"),
                    currency="RUB",
                    supplier_id_snapshot=seeded.supplier_id,
                ),
                OrderItem(
                    id=uuid.uuid4(),
                    order_id=order.id,
                    product_id=1002,
                    product_name_snapshot="QA Mouse",
                    quantity=1,
                    unit_price=Decimal("499.99"),
                    line_total=Decimal("499.99"),
                    currency="RUB",
                    supplier_id_snapshot=seeded.supplier_id,
                ),
                OrderStatusHistory(
                    id=uuid.uuid4(),
                    order_id=order.id,
                    business_status_before=None,
                    business_status_after=BusinessStatus.PENDING_RESERVATION,
                    operation_state_before=None,
                    operation_state_after=OperationState.NONE,
                    reservation_state_before=None,
                    reservation_state_after=ReservationState.REQUESTED,
                    trigger="CREATE_ORDER",
                    actor_type=ActorType.CUSTOMER,
                    actor_id=101,
                    event_id=None,
                    correlation_id=seeded.correlation_id,
                    version_before=0,
                    version_after=1,
                    created_at=NOW,
                ),
            ]
        )
    return seeded


def _envelope(
    seeded: SeededOrder,
    *,
    event_type: str = SUCCESS_EVENT,
    event_id: uuid.UUID | None = None,
    reservation_id: uuid.UUID | None = None,
    reserved_items: list[dict] | None = None,
    reason_code: str = "INSUFFICIENT_STOCK",
    failed_items: list[dict] | None = None,
    supplier_id: int | None = None,
    reservation_request_id: uuid.UUID | None = None,
    correlation_id: uuid.UUID | None = None,
) -> dict:
    event_id = event_id or uuid.uuid4()
    correlation_id = correlation_id or seeded.correlation_id
    payload: dict = {
        "order_id": str(seeded.order_id),
        "reservation_request_id": str(
            reservation_request_id or seeded.reservation_request_id
        ),
        "supplier_id": supplier_id or seeded.supplier_id,
    }
    if event_type == SUCCESS_EVENT:
        payload.update(
            {
                "reservation_id": str(reservation_id or uuid.uuid4()),
                "reserved_items": (
                    reserved_items
                    if reserved_items is not None
                    else [
                        {"product_id": 1001, "quantity": 2},
                        {"product_id": 1002, "quantity": 1},
                    ]
                ),
                "reserved_at": _timestamp(NOW + timedelta(seconds=1)),
            }
        )
    else:
        payload.update(
            {
                "failure_category": "BUSINESS",
                "reason_code": reason_code,
                "failed_items": (
                    failed_items
                    if failed_items is not None
                    else [
                        {
                            "product_id": 1001,
                            "requested_quantity": 2,
                            "available_quantity": 1,
                        }
                    ]
                ),
                "occurred_at": _timestamp(NOW + timedelta(seconds=1)),
                "retryable": False,
            }
        )
    return {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": _timestamp(NOW + timedelta(seconds=1)),
        "producer": "supplier-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(seeded.order_id),
        "correlation_id": str(correlation_id),
        "causation_id": str(uuid.uuid4()),
        "payload": payload,
    }


def _headers(envelope: dict) -> dict[str, str | None]:
    return {
        "event_id": envelope["event_id"],
        "event_type": envelope["event_type"],
        "event_version": str(envelope["event_version"]),
        "correlation_id": envelope["correlation_id"],
        "causation_id": envelope["causation_id"],
        "producer": envelope["producer"],
        "content_type": "application/json",
    }


def _command(seeded: SeededOrder, **kwargs):
    envelope = _envelope(seeded, **kwargs)
    return parse_stock_result(
        envelope,
        headers=_headers(envelope),
        key=str(seeded.order_id),
    )


def _service(factory, **kwargs) -> StockResultService:
    return StockResultService(
        session_factory=factory,
        consumer_name=CONSUMER_NAME,
        clock=lambda: NOW + timedelta(seconds=2),
        **kwargs,
    )


@pytest.mark.integration
def test_success_transitions_order_with_history_outbox_and_processed_inbox(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(seeded)

    result = _service(stock_result_session_factory).process(command)

    assert result.result == "RESERVED"
    assert result.version_before == 1
    assert result.version_after == 2
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order.business_status == BusinessStatus.RESERVED
        assert order.operation_state == OperationState.NONE
        assert order.reservation_state == ReservationState.RESERVED
        assert order.reservation_id == command.reservation_id
        assert order.target_terminal_status is None
        assert order.failure_phase is None
        assert order.failure_reason_code is None
        assert order.version == 2

        history = session.scalar(
            select(OrderStatusHistory)
            .where(OrderStatusHistory.version_after == 2)
        )
        assert history.business_status_before == BusinessStatus.PENDING_RESERVATION
        assert history.business_status_after == BusinessStatus.RESERVED
        assert history.reservation_state_before == ReservationState.REQUESTED
        assert history.reservation_state_after == ReservationState.RESERVED
        assert history.trigger == SUCCESS_EVENT
        assert history.actor_type == ActorType.KAFKA_CONSUMER
        assert history.event_id == command.event_id
        assert history.correlation_id == seeded.correlation_id
        assert history.version_before == 1
        assert history.version_after == 2

        outbox = session.scalar(
            select(OrderOutbox)
            .where(OrderOutbox.event_type == "OrderReserved")
        )
        assert outbox.status == OutboxStatus.PENDING
        assert outbox.payload["causation_id"] == str(command.event_id)
        assert outbox.payload["correlation_id"] == str(seeded.correlation_id)
        assert outbox.payload["payload"]["reservation_request_id"] == str(
            seeded.reservation_request_id
        )
        assert outbox.payload["payload"]["reservation_id"] == str(
            command.reservation_id
        )
        assert outbox.payload["payload"]["order_version"] == 2

        inbox = session.get(OrderInbox, (command.event_id, CONSUMER_NAME))
        assert inbox.status == InboxStatus.PROCESSED
        assert inbox.processed_at == NOW + timedelta(seconds=2)


@pytest.mark.integration
@pytest.mark.parametrize(
    "reason_code",
    [
        "INSUFFICIENT_STOCK",
        "PRODUCT_NOT_FOUND",
        "PRODUCT_INACTIVE",
        "PRODUCT_ARCHIVED",
        "INVALID_REQUEST",
        "SUPPLIER_MISMATCH",
        "RESERVATION_DEADLINE_EXPIRED",
    ],
)
def test_business_failure_transitions_to_rejected(
    stock_result_session_factory,
    reason_code: str,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(
        seeded,
        event_type=FAILURE_EVENT,
        reason_code=reason_code,
    )

    result = _service(stock_result_session_factory).process(command)

    assert result.result == "REJECTED"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order.business_status == BusinessStatus.REJECTED
        assert order.operation_state == OperationState.NONE
        assert order.reservation_state == ReservationState.NOT_RESERVED
        assert order.rejection_reason_code == reason_code
        assert order.rejection_reason_text is None
        assert order.failure_phase is None
        assert order.failure_reason_code is None
        assert order.version == 2

        history = session.scalar(
            select(OrderStatusHistory)
            .where(OrderStatusHistory.version_after == 2)
        )
        assert history.trigger == FAILURE_EVENT
        assert history.reason_code == reason_code
        assert history.event_id == command.event_id

        outbox = session.scalar(
            select(OrderOutbox)
            .where(OrderOutbox.event_type == "OrderRejected")
        )
        assert outbox.payload["causation_id"] == str(command.event_id)
        assert outbox.payload["payload"]["reason_code"] == reason_code
        assert outbox.payload["payload"]["order_version"] == 2

        inbox = session.get(OrderInbox, (command.event_id, CONSUMER_NAME))
        assert inbox.status == InboxStatus.PROCESSED


@pytest.mark.integration
def test_duplicate_event_is_no_op_for_version_history_and_outbox(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(seeded)
    service = _service(stock_result_session_factory)

    first = service.process(command)
    duplicate = service.process(command)

    assert first.result == "RESERVED"
    assert duplicate.result == "DUPLICATE_EVENT"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order.version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
def test_concurrent_duplicate_event_is_applied_once(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(seeded)
    service = _service(stock_result_session_factory)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(service.process, [command, command]))

    assert {result.result for result in results} == {
        "RESERVED",
        "DUPLICATE_EVENT",
    }
    with stock_result_session_factory() as session:
        assert session.get(Order, seeded.order_id).version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
def test_same_event_id_with_different_payload_is_conflict(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    event_id = uuid.uuid4()
    service = _service(stock_result_session_factory)
    service.process(_command(seeded, event_id=event_id))

    with pytest.raises(IncompatibleStockResultError) as error:
        service.process(
            _command(
                seeded,
                event_id=event_id,
                reservation_id=uuid.uuid4(),
            )
        )

    assert error.value.code == "event_payload_conflict"
    with stock_result_session_factory() as session:
        assert session.get(Order, seeded.order_id).version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
def test_new_event_id_with_same_success_is_logical_no_op(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    reservation_id = uuid.uuid4()
    service = _service(stock_result_session_factory)
    first = _command(seeded, reservation_id=reservation_id)
    second = _command(seeded, reservation_id=reservation_id)

    service.process(first)
    replay = service.process(second)

    assert replay.result == "DUPLICATE_LOGICAL_SUCCESS"
    assert replay.version_before == replay.version_after == 2
    with stock_result_session_factory() as session:
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1
        assert session.get(
            OrderInbox,
            (second.event_id, CONSUMER_NAME),
        ).status == InboxStatus.PROCESSED


@pytest.mark.integration
def test_new_event_id_with_same_failure_is_logical_no_op(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    service = _service(stock_result_session_factory)
    first = _command(seeded, event_type=FAILURE_EVENT)
    second = _command(seeded, event_type=FAILURE_EVENT)

    service.process(first)
    replay = service.process(second)

    assert replay.result == "DUPLICATE_LOGICAL_FAILURE"
    with stock_result_session_factory() as session:
        assert session.get(Order, seeded.order_id).version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
@pytest.mark.parametrize(
    "reserved_items",
    [
        [{"product_id": 1001, "quantity": 2}],
        [
            {"product_id": 1001, "quantity": 2},
            {"product_id": 1002, "quantity": 1},
            {"product_id": 1003, "quantity": 1},
        ],
        [
            {"product_id": 1001, "quantity": 1},
            {"product_id": 1002, "quantity": 1},
        ],
    ],
)
def test_success_item_mismatch_rolls_back(
    stock_result_session_factory,
    reserved_items: list[dict],
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(seeded, reserved_items=reserved_items)

    with pytest.raises(
        IncompatibleStockResultError,
        match="Reserved items do not match",
    ):
        _service(stock_result_session_factory).process(command)

    _assert_pending_without_result_effects(stock_result_session_factory, seeded)


@pytest.mark.integration
@pytest.mark.parametrize(
    "command_kwargs,error_code",
    [
        ({"supplier_id": 202}, "supplier_mismatch"),
        (
            {"reservation_request_id": uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")},
            "reservation_request_mismatch",
        ),
        (
            {"correlation_id": uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")},
            "correlation_mismatch",
        ),
    ],
)
def test_order_link_mismatch_rolls_back(
    stock_result_session_factory,
    command_kwargs: dict,
    error_code: str,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(seeded, **command_kwargs)

    with pytest.raises(IncompatibleStockResultError) as error:
        _service(stock_result_session_factory).process(command)

    assert error.value.code == error_code
    _assert_pending_without_result_effects(stock_result_session_factory, seeded)


@pytest.mark.integration
def test_reserved_then_failed_is_conflict_without_second_transition(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    service = _service(stock_result_session_factory)
    service.process(_command(seeded))

    with pytest.raises(IncompatibleStockResultError) as error:
        service.process(_command(seeded, event_type=FAILURE_EVENT))

    assert error.value.code == "conflicting_order_state"
    with stock_result_session_factory() as session:
        assert session.get(Order, seeded.order_id).business_status == BusinessStatus.RESERVED
        assert session.get(Order, seeded.order_id).version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
def test_rejected_then_success_is_conflict_without_second_transition(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    service = _service(stock_result_session_factory)
    service.process(_command(seeded, event_type=FAILURE_EVENT))

    with pytest.raises(IncompatibleStockResultError) as error:
        service.process(_command(seeded))

    assert error.value.code == "conflicting_order_state"
    with stock_result_session_factory() as session:
        assert session.get(Order, seeded.order_id).business_status == BusinessStatus.REJECTED
        assert session.get(Order, seeded.order_id).version == 2


@pytest.mark.integration
def test_late_success_never_returns_cancelled_order_to_reserved(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    with stock_result_session_factory.begin() as session:
        order = session.get(Order, seeded.order_id)
        order.business_status = BusinessStatus.CANCELLED
        order.reservation_state = ReservationState.RELEASED

    command = _command(seeded)
    result = _service(stock_result_session_factory).process(command)

    assert result.result == "LATE_SUCCESS_DEFERRED"
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order.business_status == BusinessStatus.CANCELLED
        assert order.reservation_state == ReservationState.RELEASED
        assert order.version == 1
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 0
        assert session.get(
            OrderInbox,
            (command.event_id, CONSUMER_NAME),
        ).status == InboxStatus.PROCESSED


@pytest.mark.integration
def test_concurrent_success_and_failure_only_one_transition_wins(
    stock_result_session_factory,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    service = _service(stock_result_session_factory)
    commands = [
        _command(seeded),
        _command(seeded, event_type=FAILURE_EVENT),
    ]

    def process(command):
        try:
            return service.process(command).result
        except IncompatibleStockResultError:
            return "CONFLICT"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(process, commands))

    assert "CONFLICT" in results
    assert len({"RESERVED", "REJECTED"}.intersection(results)) == 1
    with stock_result_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order.version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
@pytest.mark.parametrize("failing_component", ["history", "outbox"])
def test_transition_rolls_back_when_history_or_outbox_fails(
    stock_result_session_factory,
    failing_component: str,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)

    def fail(**_kwargs):
        raise RuntimeError(f"{failing_component} failed")

    kwargs = (
        {"history_builder": fail}
        if failing_component == "history"
        else {"outbox_builder": fail}
    )
    with pytest.raises(RuntimeError, match=f"{failing_component} failed"):
        _service(stock_result_session_factory, **kwargs).process(_command(seeded))

    _assert_pending_without_result_effects(stock_result_session_factory, seeded)


@pytest.mark.integration
@pytest.mark.parametrize("event_type", [SUCCESS_EVENT, FAILURE_EVENT])
def test_resulting_order_event_is_published_with_order_key(
    stock_result_session_factory,
    event_type: str,
) -> None:
    seeded = _seed_pending_order(stock_result_session_factory)
    command = _command(seeded, event_type=event_type)
    _service(stock_result_session_factory).process(command)
    producer = FakeProducer()
    publisher = OutboxPublisher(
        session_factory=stock_result_session_factory,
        producer=producer,
        config=OutboxPublisherConfig(
            poll_interval_seconds=1,
            batch_size=100,
            claim_lease_seconds=30,
            base_retry_delay_seconds=1,
            max_retry_delay_seconds=30,
            max_attempts=10,
            topics_by_event_type={
                "OrderReserved": "marketplace.order.events.v1",
                "OrderRejected": "marketplace.order.events.v1",
            },
        ),
        worker_id="result-publisher-test",
        clock=lambda: NOW + timedelta(seconds=3),
    )

    assert publisher.process_once() == 1

    assert producer.calls[0]["topic"] == "marketplace.order.events.v1"
    assert producer.calls[0]["key"] == str(seeded.order_id)
    assert producer.calls[0]["envelope"]["causation_id"] == str(command.event_id)
    with stock_result_session_factory() as session:
        outbox = session.scalar(select(OrderOutbox))
        assert outbox.status == OutboxStatus.PUBLISHED


def test_parser_rejects_same_event_with_mismatching_headers_and_key() -> None:
    seeded = SeededOrder(
        order_id=uuid.uuid4(),
        reservation_request_id=uuid.uuid4(),
        correlation_id=uuid.uuid4(),
        supplier_id=201,
    )
    envelope = _envelope(seeded)
    headers = _headers(envelope)
    headers["event_id"] = str(uuid.uuid4())

    with pytest.raises(IncompatibleStockResultError) as header_error:
        parse_stock_result(envelope, headers=headers, key=str(seeded.order_id))
    assert header_error.value.code == "header_mismatch"

    with pytest.raises(IncompatibleStockResultError) as key_error:
        parse_stock_result(
            envelope,
            headers=_headers(envelope),
            key=str(uuid.uuid4()),
        )
    assert key_error.value.code == "kafka_key_mismatch"


def _assert_pending_without_result_effects(factory, seeded: SeededOrder) -> None:
    with factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order.business_status == BusinessStatus.PENDING_RESERVATION
        assert order.operation_state == OperationState.NONE
        assert order.reservation_state == ReservationState.REQUESTED
        assert order.version == 1
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 0
        assert session.scalar(
            select(func.count()).select_from(OrderInbox)
        ) == 0


def _timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
