from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.config import Settings
from app.enums import (
    ActorType,
    BusinessStatus,
    InboxStatus,
    OperationState,
    ReservationState,
)
from app.models import (
    Order,
    OrderInbox,
    OrderItem,
    OrderOutbox,
    OrderStatusHistory,
)
from app.services.stock_result import FAILURE_EVENT, StockResultService
from app.stock_events_consumer import (
    IncomingKafkaMessage,
    StockEventsConsumerWorker,
    StockResultMessageHandler,
)

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
CONSUMER_NAME = "order-service-stock-events-v1"


class FakeProducer:
    def __init__(self, *, failure: Exception | None = None) -> None:
        self.calls: list[dict] = []
        self.failure = failure
        self.closed = False

    def publish(self, **kwargs) -> None:
        if self.failure is not None:
            raise self.failure
        self.calls.append(copy.deepcopy(kwargs))

    def close(self) -> None:
        self.closed = True


class FakeConsumer:
    def __init__(self, *, on_commit=None) -> None:
        self.commit_calls: list[object] = []
        self.on_commit = on_commit

    def commit(self, *, message, asynchronous=False) -> None:
        assert asynchronous is False
        if self.on_commit is not None:
            self.on_commit()
        self.commit_calls.append(message)


class FakeKafkaMessage:
    def __init__(
        self,
        *,
        envelope: dict | None = None,
        raw_value: bytes | None = None,
        key: str | None = None,
        offset: int = 42,
    ) -> None:
        if envelope is not None:
            self._value = json.dumps(envelope).encode("utf-8")
            self._headers = [
                (name, None if value is None else str(value).encode("utf-8"))
                for name, value in _headers(envelope).items()
            ]
            self._key = (key or envelope["aggregate_id"]).encode("utf-8")
        else:
            self._value = raw_value
            self._headers = []
            self._key = None if key is None else key.encode("utf-8")
        self._offset = offset

    def topic(self) -> str:
        return "marketplace.stock.events.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes | None]]:
        return self._headers


class AlwaysFailingService:
    def __init__(self) -> None:
        self.process_attempts = 0
        self.retry_attempts: list[int] = []
        self.terminal_attempts: list[int] = []

    def process(self, _command):
        self.process_attempts += 1
        raise RuntimeError("temporary database failure")

    def record_retryable_failure(self, *, command, safe_error, attempt) -> None:
        assert command.order_id
        assert safe_error == "Stock result processing failed"
        self.retry_attempts.append(attempt)

    def mark_terminal_failure(self, *, command, safe_error, attempt) -> None:
        assert command.order_id
        assert safe_error == "Stock result moved to DLQ"
        self.terminal_attempts.append(attempt)


def _settings() -> Settings:
    return Settings(
        service_name="order-service",
        environment="test",
        host="127.0.0.1",
        port=8000,
        database_url="postgresql+psycopg://order_app:order_app@localhost:5434/order_db",
        log_level="WARNING",
        kafka_bootstrap_servers="localhost:9092",
        customer_service_url="http://customer-service:8001",
        readiness_timeout_seconds=1.0,
        alembic_config="alembic.ini",
        stock_events_consumer_max_attempts=3,
    )


def _seed_pending_order(factory) -> tuple[Order, uuid.UUID]:
    order = Order(
        id=uuid.uuid4(),
        customer_id=101,
        supplier_id=201,
        cart_id=uuid.uuid4(),
        cart_version=17,
        business_status=BusinessStatus.PENDING_RESERVATION,
        operation_state=OperationState.NONE,
        reservation_state=ReservationState.REQUESTED,
        target_terminal_status=None,
        total_amount=Decimal("3001.00"),
        currency="RUB",
        version=1,
        correlation_id=uuid.uuid4(),
        reservation_request_id=uuid.uuid4(),
        reservation_requested_at=NOW,
        reservation_deadline_at=NOW + timedelta(seconds=30),
        reservation_attempt_count=1,
        reservation_last_attempt_at=NOW,
        release_attempt_count=0,
        created_at=NOW,
        updated_at=NOW,
    )
    with factory.begin() as session:
        session.add(order)
        session.add(
            OrderItem(
                id=uuid.uuid4(),
                order_id=order.id,
                product_id=1001,
                product_name_snapshot="QA Laptop",
                quantity=2,
                unit_price=Decimal("1500.50"),
                line_total=Decimal("3001.00"),
                currency="RUB",
                supplier_id_snapshot=201,
            )
        )
        session.add(
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
                correlation_id=order.correlation_id,
                version_before=0,
                version_after=1,
                created_at=NOW,
            )
        )
    return order, order.reservation_request_id


def _result_envelope(
    order: Order,
    *,
    event_type: str = "StockReservationSucceeded",
    event_id: uuid.UUID | None = None,
    reservation_id: uuid.UUID | None = None,
) -> dict:
    payload: dict = {
        "order_id": str(order.id),
        "reservation_request_id": str(order.reservation_request_id),
        "supplier_id": order.supplier_id,
    }
    if event_type == "StockReservationSucceeded":
        payload.update(
            {
                "reservation_id": str(reservation_id or uuid.uuid4()),
                "reserved_items": [{"product_id": 1001, "quantity": 2}],
                "reserved_at": _timestamp(NOW + timedelta(seconds=1)),
            }
        )
    else:
        payload.update(
            {
                "failure_category": "BUSINESS",
                "reason_code": "INSUFFICIENT_STOCK",
                "failed_items": [
                    {
                        "product_id": 1001,
                        "requested_quantity": 2,
                        "available_quantity": 1,
                    }
                ],
                "occurred_at": _timestamp(NOW + timedelta(seconds=1)),
                "retryable": False,
            }
        )
    return {
        "event_id": str(event_id or uuid.uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": _timestamp(NOW + timedelta(seconds=1)),
        "producer": "supplier-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order.id),
        "correlation_id": str(order.correlation_id),
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


def _worker(
    *,
    factory,
    consumer: FakeConsumer,
    producer: FakeProducer,
    sleeper=lambda _seconds: None,
) -> StockEventsConsumerWorker:
    settings = _settings()
    service = StockResultService(
        session_factory=factory,
        consumer_name=CONSUMER_NAME,
        clock=lambda: NOW + timedelta(seconds=2),
    )
    handler = StockResultMessageHandler(
        result_service=service,
        dlq_producer=producer,
        settings=settings,
        clock=lambda: NOW + timedelta(seconds=3),
        sleeper=sleeper,
    )
    return StockEventsConsumerWorker(
        consumer=consumer,
        handler=handler,
        settings=settings,
    )


@pytest.mark.integration
def test_offset_is_committed_only_after_database_transaction(
    stock_result_session_factory,
) -> None:
    order, _ = _seed_pending_order(stock_result_session_factory)
    envelope = _result_envelope(order)

    def assert_database_committed() -> None:
        with stock_result_session_factory() as session:
            stored = session.get(Order, order.id)
            assert stored.business_status == BusinessStatus.RESERVED
            inbox = session.get(
                OrderInbox,
                (uuid.UUID(envelope["event_id"]), CONSUMER_NAME),
            )
            assert inbox.status == InboxStatus.PROCESSED

    consumer = FakeConsumer(on_commit=assert_database_committed)
    worker = _worker(
        factory=stock_result_session_factory,
        consumer=consumer,
        producer=FakeProducer(),
    )

    result = worker.process_message(FakeKafkaMessage(envelope=envelope))

    assert result.result == "RESERVED"
    assert len(consumer.commit_calls) == 1


@pytest.mark.integration
def test_redelivery_after_unknown_offset_commit_is_safe(
    stock_result_session_factory,
) -> None:
    order, _ = _seed_pending_order(stock_result_session_factory)
    envelope = _result_envelope(order)
    message = FakeKafkaMessage(envelope=envelope)
    consumer = FakeConsumer()
    worker = _worker(
        factory=stock_result_session_factory,
        consumer=consumer,
        producer=FakeProducer(),
    )

    first = worker.process_message(message)
    redelivery = worker.process_message(message)

    assert first.result == "RESERVED"
    assert redelivery.result == "DUPLICATE_EVENT"
    assert len(consumer.commit_calls) == 2
    with stock_result_session_factory() as session:
        assert session.get(Order, order.id).version == 2
        assert session.scalar(
            select(func.count()).select_from(OrderStatusHistory)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(OrderOutbox)
        ) == 1


@pytest.mark.integration
def test_business_failed_is_processed_and_never_sent_to_dlq(
    stock_result_session_factory,
) -> None:
    order, _ = _seed_pending_order(stock_result_session_factory)
    envelope = _result_envelope(order, event_type=FAILURE_EVENT)
    consumer = FakeConsumer()
    producer = FakeProducer()
    worker = _worker(
        factory=stock_result_session_factory,
        consumer=consumer,
        producer=producer,
    )

    result = worker.process_message(FakeKafkaMessage(envelope=envelope))

    assert result.result == "REJECTED"
    assert producer.calls == []
    assert len(consumer.commit_calls) == 1
    with stock_result_session_factory() as session:
        assert session.get(Order, order.id).business_status == BusinessStatus.REJECTED


def test_malformed_event_retries_then_dlqs_before_offset_commit() -> None:
    sleeps: list[float] = []
    producer = FakeProducer()
    settings = _settings()
    handler = StockResultMessageHandler(
        result_service=AlwaysFailingService(),
        dlq_producer=producer,
        settings=settings,
        clock=lambda: NOW,
        sleeper=sleeps.append,
    )
    consumer = FakeConsumer()
    worker = StockEventsConsumerWorker(
        consumer=consumer,
        handler=handler,
        settings=settings,
    )

    result = worker.process_message(
        FakeKafkaMessage(raw_value=b"{not-json", key=str(uuid.uuid4()))
    )

    assert result.result == "DLQ"
    assert result.attempts == 3
    assert sleeps == [1, 5]
    assert len(producer.calls) == 1
    assert producer.calls[0]["topic"] == "marketplace.order.dlq.v1"
    failure = producer.calls[0]["envelope"]["failure"]
    assert failure["code"] == "invalid_json"
    assert failure["attempt_count"] == 3
    assert len(consumer.commit_calls) == 1


def test_technical_failure_retries_then_dlqs() -> None:
    order = type(
        "OrderReference",
        (),
        {
            "id": uuid.uuid4(),
            "supplier_id": 201,
            "reservation_request_id": uuid.uuid4(),
            "correlation_id": uuid.uuid4(),
        },
    )()
    envelope = _result_envelope(order)
    service = AlwaysFailingService()
    producer = FakeProducer()
    sleeps: list[float] = []
    handler = StockResultMessageHandler(
        result_service=service,
        dlq_producer=producer,
        settings=_settings(),
        clock=lambda: NOW,
        sleeper=sleeps.append,
    )
    incoming = IncomingKafkaMessage(
        topic="marketplace.stock.events.v1",
        partition=1,
        offset=11,
        key=str(order.id),
        value=json.dumps(envelope).encode("utf-8"),
        headers=_headers(envelope),
    )

    result = handler.handle(incoming)

    assert result.result == "DLQ"
    assert service.process_attempts == 3
    assert service.retry_attempts == [1, 2, 3]
    assert service.terminal_attempts == [3]
    assert sleeps == [1, 5]
    assert producer.calls[0]["envelope"]["failure"]["category"] == "TECHNICAL_FAILURE"


def test_failed_dlq_publication_prevents_offset_commit() -> None:
    settings = _settings()
    handler = StockResultMessageHandler(
        result_service=AlwaysFailingService(),
        dlq_producer=FakeProducer(failure=RuntimeError("Kafka unavailable")),
        settings=settings,
        clock=lambda: NOW,
        sleeper=lambda _seconds: None,
    )
    consumer = FakeConsumer()
    worker = StockEventsConsumerWorker(
        consumer=consumer,
        handler=handler,
        settings=settings,
    )

    with pytest.raises(RuntimeError, match="Kafka unavailable"):
        worker.process_message(FakeKafkaMessage(raw_value=b"{"))

    assert consumer.commit_calls == []


def _timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
