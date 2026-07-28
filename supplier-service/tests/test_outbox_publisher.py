from __future__ import annotations

import copy
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from app.models import SupplierOutbox
from app.services.outbox_publisher import (
    SupplierOutboxPublisher,
    SupplierOutboxPublisherConfig,
)

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=timezone.utc)
STOCK_EVENTS_TOPIC = "marketplace.stock.events.v1"


class FakeProducer:
    def __init__(self, *, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[dict] = []
        self.closed = False

    def publish(self, **kwargs) -> None:
        self.calls.append(copy.deepcopy(kwargs))
        if self.failures:
            self.failures -= 1
            raise RuntimeError("Kafka unavailable")

    def close(self) -> None:
        self.closed = True


def _config(**overrides) -> SupplierOutboxPublisherConfig:
    base = SupplierOutboxPublisherConfig(
        batch_size=100,
        claim_lease_seconds=30,
        base_retry_delay_seconds=1,
        max_retry_delay_seconds=30,
        max_attempts=10,
        topics_by_event_type={
            "StockReservationSucceeded": STOCK_EVENTS_TOPIC,
            "StockReservationFailed": STOCK_EVENTS_TOPIC,
        },
    )
    return replace(base, **overrides)


def _seed_outbox(
    factory,
    *,
    event_type: str,
    status: str = "PENDING",
    attempt_count: int = 0,
    next_attempt_at: datetime = NOW,
    locked_at: datetime | None = None,
    locked_by: str | None = None,
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    event_id = uuid.uuid4()
    order_id = uuid.uuid4()
    correlation_id = uuid.uuid4()
    envelope = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": "2026-07-28T12:00:00.000Z",
        "producer": "supplier-service",
        "aggregate_type": "ORDER",
        "aggregate_id": str(order_id),
        "correlation_id": str(correlation_id),
        "causation_id": str(uuid.uuid4()),
        "payload": {
            "order_id": str(order_id),
            "reservation_request_id": str(uuid.uuid4()),
            "supplier_id": 201,
        },
    }
    headers = {
        "event_id": str(event_id),
        "event_type": event_type,
        "event_version": "1",
        "correlation_id": str(correlation_id),
        "causation_id": envelope["causation_id"],
        "producer": "supplier-service",
        "content_type": "application/json",
    }
    with factory.begin() as session:
        session.add(
            SupplierOutbox(
                id=event_id,
                aggregate_type="ORDER",
                aggregate_id=order_id,
                event_type=event_type,
                event_version=1,
                payload=envelope,
                headers=headers,
                status=status,
                attempt_count=attempt_count,
                next_attempt_at=next_attempt_at,
                created_at=NOW,
                published_at=NOW if status == "PUBLISHED" else None,
                locked_at=locked_at,
                locked_by=locked_by,
            )
        )
    return event_id, order_id, correlation_id


def _publisher(factory, producer, *, now=NOW, worker_id="supplier-publisher-a", config=None):
    return SupplierOutboxPublisher(
        session_factory=factory,
        producer=producer,
        config=config or _config(),
        worker_id=worker_id,
        clock=lambda: now,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "event_type",
    ["StockReservationSucceeded", "StockReservationFailed"],
)
def test_supplier_outbox_publishes_contract_results(
    supplier_session_factory,
    event_type,
) -> None:
    event_id, order_id, correlation_id = _seed_outbox(
        supplier_session_factory,
        event_type=event_type,
    )
    producer = FakeProducer()

    assert _publisher(supplier_session_factory, producer).process_once() == 1

    assert len(producer.calls) == 1
    call = producer.calls[0]
    assert call["topic"] == STOCK_EVENTS_TOPIC
    assert call["key"] == str(order_id)
    assert call["value"]["event_id"] == str(event_id)
    assert call["value"]["correlation_id"] == str(correlation_id)
    assert call["headers"]["event_id"] == str(event_id)
    with supplier_session_factory() as session:
        record = session.get(SupplierOutbox, event_id)
        assert record.status == "PUBLISHED"
        assert record.attempt_count == 1
        assert record.published_at == NOW
        assert record.locked_at is None
        assert record.locked_by is None


@pytest.mark.integration
def test_supplier_outbox_failure_schedules_retry_with_same_event(
    supplier_session_factory,
) -> None:
    event_id, order_id, _ = _seed_outbox(
        supplier_session_factory,
        event_type="StockReservationSucceeded",
    )
    producer = FakeProducer(failures=1)

    _publisher(supplier_session_factory, producer).process_once()

    with supplier_session_factory() as session:
        record = session.get(SupplierOutbox, event_id)
        assert record.status == "PENDING"
        assert record.attempt_count == 1
        assert record.next_attempt_at == NOW + timedelta(seconds=1)
        assert record.last_error == "Kafka delivery failed"
        assert record.aggregate_id == order_id
        assert record.payload["event_id"] == str(event_id)


@pytest.mark.integration
def test_published_supplier_outbox_is_not_published_again(
    supplier_session_factory,
) -> None:
    _seed_outbox(
        supplier_session_factory,
        event_type="StockReservationSucceeded",
        status="PUBLISHED",
        attempt_count=1,
    )
    producer = FakeProducer()

    assert _publisher(supplier_session_factory, producer).process_once() == 0
    assert producer.calls == []


@pytest.mark.integration
def test_expired_supplier_outbox_claim_is_recovered(supplier_session_factory) -> None:
    event_id, _, _ = _seed_outbox(
        supplier_session_factory,
        event_type="StockReservationFailed",
        status="IN_PROGRESS",
        attempt_count=1,
        next_attempt_at=NOW - timedelta(minutes=1),
        locked_at=NOW - timedelta(seconds=31),
        locked_by="crashed-worker",
    )
    producer = FakeProducer()

    _publisher(
        supplier_session_factory,
        producer,
        worker_id="recovery-worker",
    ).process_once()

    with supplier_session_factory() as session:
        record = session.get(SupplierOutbox, event_id)
        assert record.status == "PUBLISHED"
        assert record.attempt_count == 2
