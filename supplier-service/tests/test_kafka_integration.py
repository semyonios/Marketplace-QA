from __future__ import annotations

import json
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import select

from app.config import Settings
from app.messaging.kafka import ConfluentJsonProducer
from app.models import Product, StockReservation, Supplier, SupplierInbox, SupplierOutbox
from app.services.outbox_publisher import (
    SupplierOutboxPublisher,
    SupplierOutboxPublisherConfig,
)
from app.services.reservation import ReservationService
from app.stock_consumer import (
    ReservationConsumerWorker,
    ReservationMessageHandler,
)


@pytest.mark.integration
def test_real_kafka_reservation_command_and_result_round_trip(
    supplier_session_factory,
) -> None:
    bootstrap_servers = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        pytest.skip("TEST_KAFKA_BOOTSTRAP_SERVERS is required")

    now = datetime.now(timezone.utc)
    event_id = uuid.uuid4()
    order_id = uuid.uuid4()
    request_id = uuid.uuid4()
    correlation_id = uuid.uuid4()
    topic_suffix = uuid.uuid4().hex
    envelope = {
        "event_id": str(event_id),
        "event_type": "StockReservationRequested",
        "event_version": 1,
        "occurred_at": _timestamp(now),
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
            "reservation_request_id": str(request_id),
            "items": [{"product_id": 1001, "quantity": 2}],
            "reservation_deadline_at": _timestamp(now + timedelta(seconds=30)),
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
    with supplier_session_factory.begin() as session:
        session.add(
            Supplier(
                id=201,
                full_name="Kafka Supplier",
                phone_number="+79990000001",
                email="kafka-supplier@example.test",
                birth_date=date(1990, 1, 1),
                city="Moscow",
                created_at=now,
                updated_at=now,
            )
        )
        session.flush()
        session.add(
            Product(
                id=1001,
                supplier_id=201,
                name="Kafka Product",
                description=None,
                price=100.0,
                stocks=5,
                reserved_stocks=0,
                is_active=True,
                is_archived=False,
                created_at=now,
            )
        )

    settings = Settings(
        service_name="supplier-service",
        environment="test",
        database_url="postgresql+psycopg://app:app@localhost:5432/supplier_db",
        kafka_bootstrap_servers=bootstrap_servers,
        alembic_config="alembic.ini",
        stock_commands_topic=f"marketplace.stock.commands.v1.test-{topic_suffix}",
        stock_events_topic=f"marketplace.stock.events.v1.test-{topic_suffix}",
    )
    _ensure_topics(
        bootstrap_servers,
        settings.stock_commands_topic,
        settings.stock_events_topic,
    )
    command_consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"supplier-command-integration-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    result_consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"supplier-result-integration-{uuid.uuid4()}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    command_producer = ConfluentJsonProducer(
        bootstrap_servers=bootstrap_servers,
        delivery_timeout_seconds=10,
    )
    dlq_producer = ConfluentJsonProducer(
        bootstrap_servers=bootstrap_servers,
        delivery_timeout_seconds=10,
    )
    result_producer = ConfluentJsonProducer(
        bootstrap_servers=bootstrap_servers,
        delivery_timeout_seconds=10,
    )
    handler = ReservationMessageHandler(
        reservation_service=ReservationService(
            session_factory=supplier_session_factory,
            consumer_name=settings.reservation_consumer_name,
        ),
        dlq_producer=dlq_producer,
        settings=settings,
        sleeper=lambda _: None,
    )
    worker = ReservationConsumerWorker(
        consumer=command_consumer,
        handler=handler,
        settings=settings,
    )

    command_consumer.subscribe([settings.stock_commands_topic])
    result_consumer.subscribe([settings.stock_events_topic])
    try:
        _wait_for_assignment(command_consumer)
        _wait_for_assignment(result_consumer)
        command_producer.publish(
            topic=settings.stock_commands_topic,
            key=str(order_id),
            value=envelope,
            headers=headers,
        )
        command_message = _poll_for_event(command_consumer, event_id)
        processing = worker.process_message(command_message)
        assert processing.result == "RESERVED"

        with supplier_session_factory() as session:
            inbox = session.get(
                SupplierInbox,
                (event_id, settings.reservation_consumer_name),
            )
            reservation = session.scalar(
                select(StockReservation)
                .where(StockReservation.reservation_request_id == request_id)
            )
            outbox = session.scalar(
                select(SupplierOutbox)
                .where(SupplierOutbox.aggregate_id == order_id)
            )
            assert inbox.status == "PROCESSED"
            assert reservation.status == "RESERVED"
            assert session.get(Product, 1001).reserved_stocks == 2
            result_event_id = outbox.id

        publisher = SupplierOutboxPublisher(
            session_factory=supplier_session_factory,
            producer=result_producer,
            config=SupplierOutboxPublisherConfig(
                batch_size=100,
                claim_lease_seconds=30,
                base_retry_delay_seconds=1,
                max_retry_delay_seconds=30,
                max_attempts=10,
                topics_by_event_type={
                    "StockReservationSucceeded": settings.stock_events_topic,
                    "StockReservationFailed": settings.stock_events_topic,
                },
            ),
            worker_id="real-kafka-supplier-publisher",
        )
        assert publisher.process_once() == 1

        result_message = _poll_for_event(result_consumer, result_event_id)
        result_envelope = json.loads(result_message.value().decode())
        result_headers = {
            name: None if value is None else value.decode()
            for name, value in result_message.headers()
        }
        assert result_message.key().decode() == str(order_id)
        assert result_envelope["event_type"] == "StockReservationSucceeded"
        assert result_envelope["event_id"] == str(result_event_id)
        assert result_envelope["causation_id"] == str(event_id)
        assert result_envelope["payload"]["reserved_items"] == [
            {"product_id": 1001, "quantity": 2}
        ]
        assert result_headers["event_id"] == str(result_event_id)
        assert result_headers["correlation_id"] == str(correlation_id)
        with supplier_session_factory() as session:
            assert session.get(SupplierOutbox, result_event_id).status == "PUBLISHED"
    finally:
        command_consumer.close()
        result_consumer.close()
        command_producer.close()
        handler.close()
        result_producer.close()


def _wait_for_assignment(consumer: Consumer) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        consumer.poll(0.25)
        if consumer.assignment():
            return
    raise AssertionError("Kafka consumer assignment timed out")


def _ensure_topics(bootstrap_servers: str, *topic_names: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    missing = [
        NewTopic(name, num_partitions=1, replication_factor=1)
        for name in topic_names
        if name not in existing
    ]
    for future in admin.create_topics(missing).values():
        future.result(timeout=10)


def _poll_for_event(consumer: Consumer, event_id: uuid.UUID):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        message = consumer.poll(1)
        if message is None or message.error():
            continue
        try:
            decoded = json.loads(message.value().decode())
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if decoded.get("event_id") == str(event_id):
            return message
    raise AssertionError(f"Kafka event {event_id} was not received")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
