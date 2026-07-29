from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest

from app.config import Settings
from app.models import Product, Supplier, SupplierInbox, SupplierOutbox
from app.services.reservation import ReservationService
from app.stock_consumer import (
    IncomingKafkaMessage,
    ReservationConsumerWorker,
    ReservationMessageHandler,
)

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)


class FakeProducer:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.closed = False

    def publish(self, **kwargs) -> None:
        self.calls.append(kwargs)

    def close(self) -> None:
        self.closed = True


class FakeKafkaMessage:
    def __init__(self, envelope: dict, headers: dict[str, str | None]) -> None:
        self.envelope = envelope
        self.header_values = headers

    def topic(self):
        return "marketplace.stock.commands.v1"

    def partition(self):
        return 1

    def offset(self):
        return 42

    def key(self):
        return self.envelope["aggregate_id"].encode()

    def value(self):
        return json.dumps(self.envelope).encode()

    def headers(self):
        return [
            (name, None if value is None else value.encode())
            for name, value in self.header_values.items()
        ]


class FakeConsumer:
    def __init__(self, on_commit=None) -> None:
        self.commits = 0
        self.on_commit = on_commit

    def commit(self, *, message, asynchronous=False):
        if self.on_commit:
            self.on_commit()
        self.commits += 1

    def subscribe(self, topics):
        pass

    def poll(self, timeout):
        return None

    def seek(self, partition):
        pass

    def close(self):
        pass


def _settings(**overrides) -> Settings:
    values = {
        "service_name": "supplier-service",
        "environment": "test",
        "database_url": "postgresql+psycopg://app:app@localhost:5432/supplier_db",
        "kafka_bootstrap_servers": "localhost:9092",
        "alembic_config": "alembic.ini",
        "consumer_max_attempts": 3,
    }
    values.update(overrides)
    return Settings(**values)


def _message() -> tuple[dict, dict[str, str | None]]:
    event_id = uuid.uuid4()
    order_id = uuid.uuid4()
    correlation_id = uuid.uuid4()
    envelope = {
        "event_id": str(event_id),
        "event_type": "StockReservationRequested",
        "event_version": 1,
        "occurred_at": "2026-07-28T12:00:00.000Z",
        "producer": "order-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order_id),
        "correlation_id": str(correlation_id),
        "causation_id": None,
        "payload": {
            "order_id": str(order_id),
            "customer_id": 101,
            "supplier_id": 201,
            "order_version": 1,
            "reservation_request_id": str(uuid.uuid4()),
            "items": [{"product_id": 1001, "quantity": 1}],
            "reservation_deadline_at": "2026-07-28T12:00:30.000Z",
            "correlation_id": str(correlation_id),
        },
    }
    headers = {
        "event_id": str(event_id),
        "event_type": "StockReservationRequested",
        "event_version": "1",
        "correlation_id": str(correlation_id),
        "causation_id": None,
        "producer": "order-service",
        "content_type": "application/json",
    }
    return envelope, headers


def _seed(factory) -> None:
    with factory.begin() as session:
        session.add(
            Supplier(
                id=201,
                full_name="Supplier",
                phone_number="+79990000001",
                email="supplier@example.test",
                birth_date=date(1990, 1, 1),
                city="Moscow",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        session.flush()
        session.add(
            Product(
                id=1001,
                supplier_id=201,
                name="Product",
                description=None,
                price=100.0,
                stocks=1,
                reserved_stocks=0,
                is_active=True,
                is_archived=False,
                created_at=NOW,
            )
        )


def _handler(factory, producer, *, sleeper=lambda _: None):
    return ReservationMessageHandler(
        reservation_service=ReservationService(
            session_factory=factory,
            consumer_name="supplier-service-stock-commands-v1",
            clock=lambda: NOW,
        ),
        dlq_producer=producer,
        settings=_settings(),
        clock=lambda: NOW,
        sleeper=sleeper,
    )


@pytest.mark.integration
def test_offset_is_committed_only_after_business_transaction(
    supplier_session_factory,
) -> None:
    _seed(supplier_session_factory)
    envelope, headers = _message()
    producer = FakeProducer()

    def assert_db_committed():
        with supplier_session_factory() as session:
            inbox = session.get(
                SupplierInbox,
                (uuid.UUID(envelope["event_id"]), "supplier-service-stock-commands-v1"),
            )
            assert inbox.status == "PROCESSED"
            assert session.get(Product, 1001).reserved_stocks == 1
            assert session.query(SupplierOutbox).count() == 1

    consumer = FakeConsumer(on_commit=assert_db_committed)
    worker = ReservationConsumerWorker(
        consumer=consumer,
        handler=_handler(supplier_session_factory, producer),
        settings=_settings(),
    )

    result = worker.process_message(FakeKafkaMessage(envelope, headers))

    assert result.result == "RESERVED"
    assert consumer.commits == 1
    assert producer.calls == []


@pytest.mark.integration
def test_duplicate_after_unknown_offset_commit_is_safe(supplier_session_factory) -> None:
    _seed(supplier_session_factory)
    envelope, headers = _message()
    producer = FakeProducer()
    consumer = FakeConsumer()
    worker = ReservationConsumerWorker(
        consumer=consumer,
        handler=_handler(supplier_session_factory, producer),
        settings=_settings(),
    )
    message = FakeKafkaMessage(envelope, headers)

    first = worker.process_message(message)
    duplicate = worker.process_message(message)

    assert first.result == "RESERVED"
    assert duplicate.result == "DUPLICATE_EVENT"
    assert consumer.commits == 2
    with supplier_session_factory() as session:
        assert session.get(Product, 1001).reserved_stocks == 1
        assert session.query(SupplierOutbox).count() == 1


@pytest.mark.integration
def test_poison_message_goes_to_dlq_before_offset_commit(
    supplier_session_factory,
) -> None:
    producer = FakeProducer()
    sleeps: list[float] = []
    handler = _handler(
        supplier_session_factory,
        producer,
        sleeper=sleeps.append,
    )
    incoming = IncomingKafkaMessage(
        topic="marketplace.stock.commands.v1",
        partition=0,
        offset=10,
        key="bad-order",
        value=b"{not-json",
        headers={},
    )

    result = handler.handle(incoming)

    assert result.result == "DLQ"
    assert result.attempts == 3
    assert sleeps == [1, 5]
    assert len(producer.calls) == 1
    call = producer.calls[0]
    assert call["topic"] == "marketplace.order.dlq.v1"
    assert call["key"] == "bad-order"
    assert call["value"]["failure"]["category"] == "INCOMPATIBLE_MESSAGE"
    assert call["value"]["failure"]["attempt_count"] == 3
    assert call["value"]["original_offset"] == 10


@pytest.mark.integration
def test_dlq_is_published_before_worker_commits_poison_offset(
    supplier_session_factory,
) -> None:
    envelope, headers = _message()
    envelope["event_version"] = 99
    headers["event_version"] = "99"
    producer = FakeProducer()

    def assert_dlq_published():
        assert len(producer.calls) == 1

    consumer = FakeConsumer(on_commit=assert_dlq_published)
    worker = ReservationConsumerWorker(
        consumer=consumer,
        handler=_handler(supplier_session_factory, producer),
        settings=_settings(),
    )

    result = worker.process_message(FakeKafkaMessage(envelope, headers))

    assert result.result == "DLQ"
    assert consumer.commits == 1


class AlwaysFailingService:
    def __init__(self) -> None:
        self.retry_attempts: list[int] = []
        self.dlq_attempt: int | None = None

    def process(self, command):
        raise RuntimeError("database unavailable")

    def record_retryable_failure(self, *, command, safe_error, attempt):
        self.retry_attempts.append(attempt)

    def mark_dlq(self, *, command, safe_error, attempt):
        self.dlq_attempt = attempt


def test_technical_failure_retries_then_publishes_dlq() -> None:
    envelope, headers = _message()
    failing = AlwaysFailingService()
    producer = FakeProducer()
    handler = ReservationMessageHandler(
        reservation_service=failing,
        dlq_producer=producer,
        settings=_settings(),
        clock=lambda: NOW,
        sleeper=lambda _: None,
    )

    result = handler.handle(
        IncomingKafkaMessage(
            topic="marketplace.stock.commands.v1",
            partition=1,
            offset=42,
            key=envelope["aggregate_id"],
            value=json.dumps(envelope).encode(),
            headers=headers,
        )
    )

    assert result.result == "DLQ"
    assert failing.retry_attempts == [1, 2, 3]
    assert failing.dlq_attempt == 3
    assert producer.calls[0]["value"]["failure"]["category"] == "TECHNICAL_FAILURE"
