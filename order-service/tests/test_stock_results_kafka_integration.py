from __future__ import annotations

import json
import os
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from time import monotonic

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import func, select

from app.config import Settings
from app.enums import (
    ActorType,
    BusinessStatus,
    InboxStatus,
    OperationState,
    OutboxStatus,
    ReservationState,
)
from app.messaging.kafka_producer import ConfluentKafkaProducer
from app.models import (
    Order,
    OrderInbox,
    OrderItem,
    OrderOutbox,
    OrderStatusHistory,
)
from app.services.outbox_publisher import OutboxPublisher, OutboxPublisherConfig
from app.services.stock_result import StockResultService
from app.stock_events_consumer import StockEventsConsumerWorker, StockResultMessageHandler

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)


def _settings(bootstrap_servers: str) -> Settings:
    return Settings(
        service_name="order-service",
        environment="test",
        host="127.0.0.1",
        port=8000,
        database_url=os.environ["TEST_DATABASE_URL"],
        log_level="WARNING",
        kafka_bootstrap_servers=bootstrap_servers,
        customer_service_url="http://customer-service:8001",
        readiness_timeout_seconds=1.0,
        alembic_config="alembic.ini",
        kafka_delivery_timeout_seconds=5.0,
        stock_events_consumer_poll_seconds=0.1,
    )


def _seed_pending_order(factory) -> Order:
    order = Order(
        id=uuid.uuid4(),
        customer_id=101,
        supplier_id=201,
        cart_id=uuid.uuid4(),
        cart_version=27,
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
    return order


def _supplier_success(order: Order) -> dict:
    event_id = uuid.uuid4()
    return {
        "event_id": str(event_id),
        "event_type": "StockReservationSucceeded",
        "event_version": 1,
        "occurred_at": _timestamp(NOW + timedelta(seconds=1)),
        "producer": "supplier-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order.id),
        "correlation_id": str(order.correlation_id),
        "causation_id": str(uuid.uuid4()),
        "payload": {
            "order_id": str(order.id),
            "reservation_request_id": str(order.reservation_request_id),
            "reservation_id": str(uuid.uuid4()),
            "supplier_id": order.supplier_id,
            "reserved_items": [{"product_id": 1001, "quantity": 2}],
            "reserved_at": _timestamp(NOW + timedelta(seconds=1)),
        },
    }


def _headers(envelope: dict) -> dict[str, str]:
    return {
        "event_id": envelope["event_id"],
        "event_type": envelope["event_type"],
        "event_version": str(envelope["event_version"]),
        "correlation_id": envelope["correlation_id"],
        "causation_id": envelope["causation_id"],
        "producer": envelope["producer"],
        "content_type": "application/json",
    }


def _consumer(bootstrap_servers: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"order-result-test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def _poll(consumer: Consumer, timeout_seconds: float = 10.0):
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        message = consumer.poll(0.2)
        if message is None:
            continue
        if message.error():
            continue
        return message
    raise AssertionError("Kafka message was not received before timeout")


@pytest.mark.integration
def test_supplier_result_to_order_result_through_real_kafka(
    stock_result_session_factory,
) -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap_servers:
        pytest.skip("KAFKA_BOOTSTRAP_SERVERS is required")

    suffix = uuid.uuid4().hex
    stock_topic = f"marketplace.stock.events.v1.test-{suffix}"
    order_topic = f"marketplace.order.events.v1.test-{suffix}"
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topic_futures = admin.create_topics(
        [
            NewTopic(stock_topic, num_partitions=1, replication_factor=1),
            NewTopic(order_topic, num_partitions=1, replication_factor=1),
        ]
    )
    for future in topic_futures.values():
        future.result(timeout=10)

    settings = replace(
        _settings(bootstrap_servers),
        kafka_stock_events_topic=stock_topic,
        kafka_order_events_topic=order_topic,
        stock_events_consumer_group_id=f"order-result-test-{suffix}",
        stock_events_consumer_name=f"order-result-test-{suffix}",
    )
    order = _seed_pending_order(stock_result_session_factory)
    supplier_event = _supplier_success(order)
    source_producer = ConfluentKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        delivery_timeout_seconds=5,
    )
    dlq_producer = ConfluentKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        delivery_timeout_seconds=5,
    )
    source_consumer = _consumer(bootstrap_servers, stock_topic)
    result_consumer = _consumer(bootstrap_servers, order_topic)
    try:
        source_producer.publish(
            topic=stock_topic,
            key=str(order.id),
            envelope=supplier_event,
            headers=_headers(supplier_event),
        )
        source_message = _poll(source_consumer)
        service = StockResultService(
            session_factory=stock_result_session_factory,
            consumer_name=settings.stock_events_consumer_name,
            clock=lambda: NOW + timedelta(seconds=2),
        )
        handler = StockResultMessageHandler(
            result_service=service,
            dlq_producer=dlq_producer,
            settings=settings,
            clock=lambda: NOW + timedelta(seconds=3),
            sleeper=lambda _seconds: None,
        )
        worker = StockEventsConsumerWorker(
            consumer=source_consumer,
            handler=handler,
            settings=settings,
        )

        processing = worker.process_message(source_message)

        assert processing.result == "RESERVED"
        with stock_result_session_factory() as session:
            stored_order = session.get(Order, order.id)
            assert stored_order.business_status == BusinessStatus.RESERVED
            assert stored_order.version == 2
            inbox = session.get(
                OrderInbox,
                (uuid.UUID(supplier_event["event_id"]), settings.stock_events_consumer_name),
            )
            assert inbox.status == InboxStatus.PROCESSED
            assert session.scalar(
                select(func.count()).select_from(OrderStatusHistory)
            ) == 2
            result_outbox = session.scalar(
                select(OrderOutbox).where(OrderOutbox.event_type == "OrderReserved")
            )
            assert result_outbox.status == OutboxStatus.PENDING

        result_producer = ConfluentKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            delivery_timeout_seconds=5,
        )
        publisher = OutboxPublisher(
            session_factory=stock_result_session_factory,
            producer=result_producer,
            config=OutboxPublisherConfig(
                poll_interval_seconds=0.1,
                batch_size=100,
                claim_lease_seconds=30,
                base_retry_delay_seconds=1,
                max_retry_delay_seconds=30,
                max_attempts=10,
                topics_by_event_type={"OrderReserved": order_topic},
            ),
            worker_id=f"order-result-publisher-{suffix}",
            clock=lambda: NOW + timedelta(seconds=4),
        )
        try:
            assert publisher.process_once() == 1
            result_message = _poll(result_consumer)
        finally:
            result_producer.close()

        result_event = json.loads(result_message.value().decode("utf-8"))
        assert result_message.key().decode("utf-8") == str(order.id)
        assert result_event["event_type"] == "OrderReserved"
        assert result_event["aggregate_id"] == str(order.id)
        assert result_event["causation_id"] == supplier_event["event_id"]
        assert result_event["payload"]["order_version"] == 2
    finally:
        source_consumer.close()
        result_consumer.close()
        source_producer.close()
        dlq_producer.close()


def _timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
