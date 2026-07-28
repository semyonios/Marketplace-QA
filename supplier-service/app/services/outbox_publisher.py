from __future__ import annotations

import copy
import logging
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from ..messaging.kafka import JsonMessageProducer
from ..models import SupplierOutbox

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SupplierOutboxPublisherConfig:
    batch_size: int
    claim_lease_seconds: float
    base_retry_delay_seconds: float
    max_retry_delay_seconds: float
    max_attempts: int
    topics_by_event_type: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class ClaimedSupplierEvent:
    event_id: uuid.UUID
    event_type: str
    event_version: int
    aggregate_id: uuid.UUID
    payload: dict[str, Any]
    headers: dict[str, Any]
    attempt_count: int
    worker_id: str

    @property
    def correlation_id(self) -> str | None:
        value = self.payload.get("correlation_id")
        return str(value) if value is not None else None


class SupplierOutboxPublisher:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        producer: JsonMessageProducer,
        config: SupplierOutboxPublisherConfig,
        worker_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._config = config
        self._worker_id = worker_id
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def process_once(self) -> int:
        claimed = self.claim_batch(now=self._clock())
        for event in claimed:
            started_at = time.monotonic()
            topic = self._config.topics_by_event_type.get(event.event_type)
            try:
                if topic is None:
                    raise ValueError(f"Unsupported supplier outbox event: {event.event_type}")
                self._validate_event(event)
                if not self.renew_claim(event=event, now=self._clock()):
                    self._log_result(
                        event=event,
                        topic=topic,
                        result="claim_lost_before_delivery",
                        duration_ms=(time.monotonic() - started_at) * 1000,
                        level=logging.WARNING,
                    )
                    continue
                self._producer.publish(
                    topic=topic,
                    key=str(event.aggregate_id),
                    value=event.payload,
                    headers=event.headers,
                )
                published = self.mark_published(event=event, now=self._clock())
                self._log_result(
                    event=event,
                    topic=topic,
                    result="published" if published else "claim_lost_after_delivery",
                    duration_ms=(time.monotonic() - started_at) * 1000,
                )
            except Exception as exc:
                outcome = self.mark_failed_attempt(event=event, now=self._clock())
                self._log_result(
                    event=event,
                    topic=topic,
                    result=outcome,
                    duration_ms=(time.monotonic() - started_at) * 1000,
                    error_code="kafka_publish_failed",
                    level=logging.ERROR if outcome == "failed" else logging.WARNING,
                )
                logger.debug("supplier outbox exception type=%s", type(exc).__name__)
        return len(claimed)

    def claim_batch(self, *, now: datetime) -> tuple[ClaimedSupplierEvent, ...]:
        lease_expired_before = now - timedelta(seconds=self._config.claim_lease_seconds)
        due = or_(
            and_(
                SupplierOutbox.status == "PENDING",
                SupplierOutbox.next_attempt_at <= now,
            ),
            and_(
                SupplierOutbox.status == "IN_PROGRESS",
                SupplierOutbox.locked_at <= lease_expired_before,
            ),
        )
        claimed: list[ClaimedSupplierEvent] = []
        with self._session_factory.begin() as session:
            records = session.scalars(
                select(SupplierOutbox)
                .where(due)
                .order_by(
                    SupplierOutbox.next_attempt_at,
                    SupplierOutbox.created_at,
                    SupplierOutbox.id,
                )
                .limit(min(self._config.batch_size, 100))
                .with_for_update(skip_locked=True)
            )
            for record in records:
                record.status = "IN_PROGRESS"
                record.attempt_count += 1
                record.locked_at = now
                record.locked_by = self._worker_id
                record.last_error = None
                claimed.append(
                    ClaimedSupplierEvent(
                        event_id=record.id,
                        event_type=record.event_type,
                        event_version=record.event_version,
                        aggregate_id=record.aggregate_id,
                        payload=copy.deepcopy(record.payload),
                        headers=copy.deepcopy(record.headers),
                        attempt_count=record.attempt_count,
                        worker_id=self._worker_id,
                    )
                )
        return tuple(claimed)

    def renew_claim(self, *, event: ClaimedSupplierEvent, now: datetime) -> bool:
        with self._session_factory.begin() as session:
            record = session.scalar(
                select(SupplierOutbox)
                .where(SupplierOutbox.id == event.event_id)
                .with_for_update()
            )
            if not self._owns_claim(record=record, event=event):
                return False
            record.locked_at = now
        return True

    def mark_published(self, *, event: ClaimedSupplierEvent, now: datetime) -> bool:
        with self._session_factory.begin() as session:
            record = session.scalar(
                select(SupplierOutbox)
                .where(SupplierOutbox.id == event.event_id)
                .with_for_update()
            )
            if not self._owns_claim(record=record, event=event):
                return False
            record.status = "PUBLISHED"
            record.published_at = now
            record.last_error = None
            record.locked_at = None
            record.locked_by = None
        return True

    def mark_failed_attempt(self, *, event: ClaimedSupplierEvent, now: datetime) -> str:
        with self._session_factory.begin() as session:
            record = session.scalar(
                select(SupplierOutbox)
                .where(SupplierOutbox.id == event.event_id)
                .with_for_update()
            )
            if not self._owns_claim(record=record, event=event):
                return "claim_lost_after_failure"
            record.last_error = "Kafka delivery failed"
            record.locked_at = None
            record.locked_by = None
            if record.attempt_count >= self._config.max_attempts:
                record.status = "FAILED"
                return "failed"
            record.status = "PENDING"
            record.next_attempt_at = now + timedelta(
                seconds=self.retry_delay_seconds(record.attempt_count)
            )
        return "retry_scheduled"

    def retry_delay_seconds(self, attempt_count: int) -> float:
        return min(
            self._config.base_retry_delay_seconds * (2 ** max(0, attempt_count - 1)),
            self._config.max_retry_delay_seconds,
        )

    def close(self) -> None:
        self._producer.close()

    @staticmethod
    def _owns_claim(
        *,
        record: SupplierOutbox | None,
        event: ClaimedSupplierEvent,
    ) -> bool:
        return (
            record is not None
            and record.status == "IN_PROGRESS"
            and record.locked_by == event.worker_id
        )

    @staticmethod
    def _validate_event(event: ClaimedSupplierEvent) -> None:
        envelope = event.payload
        required_envelope = {
            "event_id",
            "event_type",
            "event_version",
            "occurred_at",
            "producer",
            "aggregate_type",
            "aggregate_id",
            "correlation_id",
            "causation_id",
            "payload",
        }
        if not required_envelope.issubset(envelope):
            raise ValueError("Supplier outbox envelope is incomplete")
        if (
            envelope["event_id"] != str(event.event_id)
            or envelope["event_type"] != event.event_type
            or envelope["event_version"] != event.event_version
            or envelope["aggregate_id"] != str(event.aggregate_id)
        ):
            raise ValueError("Supplier outbox identity does not match envelope")
        required_headers = {
            "event_id",
            "event_type",
            "event_version",
            "correlation_id",
            "causation_id",
            "producer",
            "content_type",
        }
        if not required_headers.issubset(event.headers):
            raise ValueError("Supplier outbox headers are incomplete")
        expected = {
            "event_id": envelope["event_id"],
            "event_type": envelope["event_type"],
            "event_version": str(envelope["event_version"]),
            "correlation_id": envelope["correlation_id"],
            "causation_id": envelope["causation_id"],
            "producer": envelope["producer"],
            "content_type": "application/json",
        }
        if any(event.headers[name] != value for name, value in expected.items()):
            raise ValueError("Supplier outbox headers do not match envelope")

    def _log_result(
        self,
        *,
        event: ClaimedSupplierEvent,
        topic: str | None,
        result: str,
        duration_ms: float,
        error_code: str | None = None,
        level: int = logging.INFO,
    ) -> None:
        logger.log(
            level,
            "supplier outbox publication result",
            extra={
                "worker": self._worker_id,
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "aggregate_id": str(event.aggregate_id),
                "order_id": str(event.aggregate_id),
                "topic": topic or "unmapped",
                "attempt_count": event.attempt_count,
                "correlation_id": event.correlation_id,
                "result": result,
                "duration_ms": round(duration_ms, 3),
                "error_code": error_code,
            },
        )


class SupplierOutboxPublisherWorker:
    def __init__(
        self,
        *,
        publisher: SupplierOutboxPublisher,
        poll_interval_seconds: float,
    ) -> None:
        self._publisher = publisher
        self._poll_interval_seconds = poll_interval_seconds
        self._stop_event = threading.Event()

    def run_forever(self) -> None:
        logger.info("supplier outbox publisher started")
        try:
            while not self._stop_event.is_set():
                try:
                    processed = self._publisher.process_once()
                except Exception:
                    logger.exception("supplier outbox polling failed")
                    processed = 0
                if processed == 0:
                    self._stop_event.wait(self._poll_interval_seconds)
        finally:
            self._publisher.close()
            logger.info("supplier outbox publisher stopped")

    def stop(self) -> None:
        self._stop_event.set()
