from __future__ import annotations

import copy
import json
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from confluent_kafka import Consumer
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

from app.config import Settings
from app.database import Base
from app.enums import (
    BusinessStatus,
    OperationState,
    OutboxStatus,
    ReservationState,
)
from app.messaging.kafka_producer import ConfluentKafkaProducer
from app.models import Order, OrderOutbox
from app.services.order_creation import (
    PreparedItem,
    PreparedOrder,
    build_outbox_records,
)
from app.services.outbox_publisher import (
    OutboxPublisher,
    OutboxPublisherConfig,
    OutboxPublisherWorker,
)

ORDER_TOPIC = "marketplace.order.events.v1"
STOCK_TOPIC = "marketplace.stock.commands.v1"


@dataclass(frozen=True, slots=True)
class SeededEvent:
    event_id: uuid.UUID
    order_id: uuid.UUID
    correlation_id: uuid.UUID
    original_payload: dict


class MutableClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def __call__(self) -> datetime:
        return self.current

    def advance(self, **kwargs) -> None:
        self.current += timedelta(**kwargs)


class FakeProducer:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[dict] = []
        self.closed = False

    def publish(self, **kwargs) -> None:
        self.calls.append(copy.deepcopy(kwargs))
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("temporary Kafka delivery failure")

    def close(self) -> None:
        self.closed = True


class InspectingProducer(FakeProducer):
    def __init__(self, factory, event_id: uuid.UUID) -> None:
        super().__init__()
        self._factory = factory
        self._event_id = event_id

    def publish(self, **kwargs) -> None:
        with self._factory() as session:
            record = session.get(OrderOutbox, self._event_id)
            assert record is not None
            assert record.status == OutboxStatus.IN_PROGRESS
            assert record.locked_by is not None
        super().publish(**kwargs)


@pytest.fixture
def outbox_session_factory():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("TEST_DATABASE_URL is required")

    schema_name = f"outbox_publisher_test_{uuid.uuid4().hex}"
    admin_engine = create_engine(database_url, future=True)
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))

    test_engine = create_engine(
        database_url,
        future=True,
        connect_args={"options": f"-csearch_path={schema_name}"},
    )
    Base.metadata.create_all(test_engine)
    factory = sessionmaker(bind=test_engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        test_engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        admin_engine.dispose()


def _config(**overrides) -> OutboxPublisherConfig:
    base = OutboxPublisherConfig(
        poll_interval_seconds=0.05,
        batch_size=100,
        claim_lease_seconds=30,
        base_retry_delay_seconds=1,
        max_retry_delay_seconds=30,
        max_attempts=10,
        topics_by_event_type={
            "OrderCreated": ORDER_TOPIC,
            "StockReservationRequested": STOCK_TOPIC,
        },
    )
    return replace(base, **overrides)


def _publisher(
    factory,
    producer,
    clock: MutableClock,
    *,
    worker_id: str = "publisher-a",
    config: OutboxPublisherConfig | None = None,
) -> OutboxPublisher:
    return OutboxPublisher(
        session_factory=factory,
        producer=producer,
        config=config or _config(),
        worker_id=worker_id,
        clock=clock,
    )


def _seed_event(
    factory,
    *,
    event_type: str,
    created_at: datetime,
    status: OutboxStatus = OutboxStatus.PENDING,
    attempt_count: int = 0,
    next_attempt_at: datetime | None = None,
    locked_at: datetime | None = None,
    locked_by: str | None = None,
) -> SeededEvent:
    order_id = uuid.uuid4()
    correlation_id = uuid.uuid4()
    order = Order(
        id=order_id,
        customer_id=101,
        supplier_id=201,
        cart_id=uuid.uuid4(),
        cart_version=(order_id.int % 9_000_000_000_000_000_000) + 1,
        business_status=BusinessStatus.PENDING_RESERVATION,
        operation_state=OperationState.NONE,
        reservation_state=ReservationState.REQUESTED,
        total_amount=Decimal("25.00"),
        currency="RUB",
        version=1,
        correlation_id=correlation_id,
        reservation_request_id=uuid.uuid4(),
        created_at=created_at,
        updated_at=created_at,
    )
    prepared = PreparedOrder(
        supplier_id=201,
        items=(
            PreparedItem(
                product_id=1001,
                product_name="Publisher product",
                supplier_id=201,
                quantity=2,
                unit_price=Decimal("12.50"),
                line_total=Decimal("25.00"),
                currency="RUB",
            ),
        ),
        total_amount=Decimal("25.00"),
        currency="RUB",
    )
    record = next(
        candidate
        for candidate in build_outbox_records(
            order=order,
            prepared=prepared,
            occurred_at=created_at,
        )
        if candidate.event_type == event_type
    )
    record.status = status
    record.attempt_count = attempt_count
    record.next_attempt_at = next_attempt_at or created_at
    record.locked_at = locked_at
    record.locked_by = locked_by
    if status == OutboxStatus.PUBLISHED:
        record.published_at = created_at

    original_payload = copy.deepcopy(record.payload)
    with factory.begin() as session:
        session.add_all([order, record])
    return SeededEvent(
        event_id=record.id,
        order_id=order_id,
        correlation_id=correlation_id,
        original_payload=original_payload,
    )


@pytest.mark.integration
def test_order_created_is_published_with_contract_topic_key_headers_and_envelope(
    outbox_session_factory,
) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    producer = FakeProducer()
    publisher = _publisher(outbox_session_factory, producer, MutableClock(now))

    assert publisher.process_once() == 1

    assert len(producer.calls) == 1
    published = producer.calls[0]
    assert published["topic"] == ORDER_TOPIC
    assert published["key"] == str(seeded.order_id)
    assert published["envelope"] == seeded.original_payload
    assert published["envelope"]["event_id"] == str(seeded.event_id)
    assert published["headers"]["event_id"] == str(seeded.event_id)
    assert published["headers"]["correlation_id"] == str(seeded.correlation_id)
    with outbox_session_factory() as session:
        record = session.get(OrderOutbox, seeded.event_id)
        assert record is not None
        assert record.status == OutboxStatus.PUBLISHED
        assert record.published_at == now
        assert record.attempt_count == 1
        assert record.locked_at is None
        assert record.locked_by is None


@pytest.mark.integration
def test_stock_reservation_is_published_and_first_claim_sets_stable_deadline(
    outbox_session_factory,
) -> None:
    now = datetime(2026, 7, 28, 12, 0, 0, 123456, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="StockReservationRequested",
        created_at=now,
    )
    producer = FakeProducer()
    publisher = _publisher(outbox_session_factory, producer, MutableClock(now))

    assert publisher.process_once() == 1

    call = producer.calls[0]
    assert call["topic"] == STOCK_TOPIC
    assert call["key"] == str(seeded.order_id)
    assert call["envelope"]["event_id"] == str(seeded.event_id)
    assert call["envelope"]["payload"]["reservation_deadline_at"] == "2026-07-28T12:00:30.123Z"
    with outbox_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order is not None
        assert order.reservation_requested_at == now
        assert order.reservation_deadline_at == now + timedelta(seconds=30)
        assert order.reservation_attempt_count == 1
        assert order.updated_at == now


@pytest.mark.integration
def test_kafka_failure_keeps_row_retryable_and_schedules_backoff(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    publisher = _publisher(
        outbox_session_factory,
        FakeProducer(failures=1),
        MutableClock(now),
    )

    assert publisher.process_once() == 1

    with outbox_session_factory() as session:
        record = session.get(OrderOutbox, seeded.event_id)
        assert record is not None
        assert record.status == OutboxStatus.PENDING
        assert record.attempt_count == 1
        assert record.next_attempt_at == now + timedelta(seconds=1)
        assert record.last_error == "Kafka delivery failed"
        assert record.published_at is None
        assert record.locked_at is None
        assert record.locked_by is None


@pytest.mark.integration
def test_row_is_not_selected_before_next_attempt_at(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
        next_attempt_at=now + timedelta(seconds=1),
    )
    producer = FakeProducer()
    publisher = _publisher(outbox_session_factory, producer, MutableClock(now))

    assert publisher.process_once() == 0
    assert producer.calls == []


@pytest.mark.integration
def test_expired_claim_is_recovered_by_another_worker(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now - timedelta(minutes=1),
        status=OutboxStatus.IN_PROGRESS,
        attempt_count=1,
        locked_at=now - timedelta(seconds=31),
        locked_by="crashed-worker",
    )
    producer = FakeProducer()
    publisher = _publisher(
        outbox_session_factory,
        producer,
        MutableClock(now),
        worker_id="recovery-worker",
    )

    assert publisher.process_once() == 1

    with outbox_session_factory() as session:
        record = session.get(OrderOutbox, seeded.event_id)
        assert record is not None
        assert record.status == OutboxStatus.PUBLISHED
        assert record.attempt_count == 2


@pytest.mark.integration
def test_two_workers_do_not_claim_the_same_row(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    barrier = threading.Barrier(3)

    def claim(worker_id: str):
        publisher = _publisher(
            outbox_session_factory,
            FakeProducer(),
            MutableClock(now),
            worker_id=worker_id,
        )
        barrier.wait()
        return publisher.claim_batch(now=now)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(claim, "worker-a")
        second = executor.submit(claim, "worker-b")
        barrier.wait()
        results = (first.result(timeout=5), second.result(timeout=5))

    assert sorted(len(result) for result in results) == [0, 1]
    assert {
        event.event_id
        for result in results
        for event in result
    } == {seeded.event_id}


@pytest.mark.integration
def test_worker_does_not_publish_a_claim_recovered_by_another_worker(
    outbox_session_factory,
) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    first = _publisher(
        outbox_session_factory,
        FakeProducer(),
        MutableClock(now),
        worker_id="worker-a",
    )
    second = _publisher(
        outbox_session_factory,
        FakeProducer(),
        MutableClock(now + timedelta(seconds=31)),
        worker_id="worker-b",
    )

    original_claim = first.claim_batch(now=now)
    recovered_claim = second.claim_batch(now=now + timedelta(seconds=31))

    assert [event.event_id for event in original_claim] == [seeded.event_id]
    assert [event.event_id for event in recovered_claim] == [seeded.event_id]
    assert first.renew_claim(event=original_claim[0], now=now + timedelta(seconds=31)) is False


@pytest.mark.integration
def test_batch_size_limits_each_claim(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    for index in range(3):
        _seed_event(
            outbox_session_factory,
            event_type="OrderCreated",
            created_at=now + timedelta(microseconds=index),
        )
    publisher = _publisher(
        outbox_session_factory,
        FakeProducer(),
        MutableClock(now + timedelta(seconds=1)),
        config=_config(batch_size=2),
    )

    claimed = publisher.claim_batch(now=now + timedelta(seconds=1))

    assert len(claimed) == 2
    with outbox_session_factory() as session:
        assert session.scalar(
            select(func.count())
            .select_from(OrderOutbox)
            .where(OrderOutbox.status == OutboxStatus.PENDING)
        ) == 1


@pytest.mark.integration
def test_published_row_is_not_published_again(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
        status=OutboxStatus.PUBLISHED,
        attempt_count=1,
    )
    producer = FakeProducer()
    publisher = _publisher(outbox_session_factory, producer, MutableClock(now))

    assert publisher.process_once() == 0
    assert producer.calls == []


@pytest.mark.integration
def test_unknown_delivery_result_retries_same_event_id_and_payload(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    clock = MutableClock(now)
    producer = FakeProducer(failures=1)
    publisher = _publisher(outbox_session_factory, producer, clock)

    assert publisher.process_once() == 1
    clock.advance(seconds=1)
    assert publisher.process_once() == 1

    assert len(producer.calls) == 2
    assert producer.calls[0]["envelope"] == producer.calls[1]["envelope"]
    assert producer.calls[0]["envelope"]["event_id"] == str(seeded.event_id)
    assert producer.calls[0]["key"] == producer.calls[1]["key"]
    with outbox_session_factory() as session:
        record = session.get(OrderOutbox, seeded.event_id)
        assert record is not None
        assert record.status == OutboxStatus.PUBLISHED
        assert record.attempt_count == 2


@pytest.mark.integration
def test_stock_retry_preserves_first_attempt_deadline_and_payload(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="StockReservationRequested",
        created_at=now,
    )
    clock = MutableClock(now)
    producer = FakeProducer(failures=1)
    publisher = _publisher(outbox_session_factory, producer, clock)

    assert publisher.process_once() == 1
    first_attempt = copy.deepcopy(producer.calls[0]["envelope"])
    clock.advance(seconds=1)
    assert publisher.process_once() == 1

    assert producer.calls[1]["envelope"] == first_attempt
    assert first_attempt["event_id"] == str(seeded.event_id)
    assert first_attempt["payload"]["reservation_deadline_at"] == "2026-07-28T12:00:30.000Z"
    with outbox_session_factory() as session:
        order = session.get(Order, seeded.order_id)
        assert order is not None
        assert order.reservation_requested_at == now
        assert order.reservation_deadline_at == now + timedelta(seconds=30)
        assert order.reservation_attempt_count == 1


@pytest.mark.integration
def test_claim_transaction_is_committed_before_network_publish(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    producer = InspectingProducer(outbox_session_factory, seeded.event_id)
    publisher = _publisher(outbox_session_factory, producer, MutableClock(now))

    assert publisher.process_once() == 1
    assert len(producer.calls) == 1


@pytest.mark.integration
def test_tenth_failed_attempt_moves_row_to_failed_without_publisher_dlq(
    outbox_session_factory,
) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
        attempt_count=9,
    )
    publisher = _publisher(
        outbox_session_factory,
        FakeProducer(failures=1),
        MutableClock(now),
    )

    assert publisher.process_once() == 1

    with outbox_session_factory() as session:
        record = session.get(OrderOutbox, seeded.event_id)
        assert record is not None
        assert record.status == OutboxStatus.FAILED
        assert record.attempt_count == 10
        assert record.published_at is None


@pytest.mark.parametrize(
    ("attempt_count", "expected_delay"),
    [(1, 1), (2, 2), (3, 4), (4, 8), (5, 16), (6, 30), (10, 30)],
)
def test_retry_delay_matches_contract(attempt_count, expected_delay) -> None:
    publisher = OutboxPublisher(
        session_factory=None,
        producer=FakeProducer(),
        config=_config(),
        worker_id="delay-test",
    )

    assert publisher.retry_delay_seconds(attempt_count) == expected_delay


def test_outbox_batch_size_cannot_exceed_contract_limit(monkeypatch) -> None:
    monkeypatch.setenv("OUTBOX_BATCH_SIZE", "101")

    with pytest.raises(ValueError, match="less than or equal to 100"):
        Settings.from_environment()


@pytest.mark.integration
def test_temporary_kafka_failure_does_not_stop_future_publication(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    clock = MutableClock(now)
    producer = FakeProducer(failures=1)
    publisher = _publisher(outbox_session_factory, producer, clock)

    publisher.process_once()
    clock.advance(seconds=1)
    publisher.process_once()

    with outbox_session_factory() as session:
        assert session.get(OrderOutbox, seeded.event_id).status == OutboxStatus.PUBLISHED


@pytest.mark.integration
def test_graceful_shutdown_stops_polling_and_closes_producer(outbox_session_factory) -> None:
    now = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
    producer = FakeProducer()
    publisher = _publisher(outbox_session_factory, producer, MutableClock(now))
    worker = OutboxPublisherWorker(publisher=publisher, poll_interval_seconds=0.05)
    thread = threading.Thread(target=worker.run_forever)

    thread.start()
    time.sleep(0.05)
    worker.stop()
    thread.join(timeout=2)

    assert thread.is_alive() is False
    assert producer.closed is True


@pytest.mark.integration
def test_real_kafka_publish_receives_expected_key_headers_and_body(outbox_session_factory) -> None:
    bootstrap_servers = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        pytest.skip("TEST_KAFKA_BOOTSTRAP_SERVERS is required")

    now = datetime.now(timezone.utc)
    seeded = _seed_event(
        outbox_session_factory,
        event_type="OrderCreated",
        created_at=now,
    )
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"order-publisher-integration-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([ORDER_TOPIC])
    consumer.poll(0.5)
    producer = ConfluentKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        delivery_timeout_seconds=10,
    )
    publisher = _publisher(
        outbox_session_factory,
        producer,
        MutableClock(now),
        worker_id="real-kafka-test",
    )

    try:
        assert publisher.process_once() == 1
        received = None
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None or message.error():
                continue
            envelope = json.loads(message.value().decode("utf-8"))
            if envelope.get("event_id") == str(seeded.event_id):
                received = message
                break
        assert received is not None
        assert received.key().decode("utf-8") == str(seeded.order_id)
        envelope = json.loads(received.value().decode("utf-8"))
        assert envelope == seeded.original_payload
        headers = {
            name: None if value is None else value.decode("utf-8")
            for name, value in received.headers()
        }
        assert headers["event_id"] == str(seeded.event_id)
        assert headers["correlation_id"] == str(seeded.correlation_id)
        assert headers["content_type"] == "application/json"
    finally:
        consumer.close()
        publisher.close()
