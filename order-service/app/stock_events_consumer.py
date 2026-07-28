from __future__ import annotations

import json
import logging
import signal
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from confluent_kafka import Consumer, TopicPartition

from .config import Settings, get_settings
from .database import SessionLocal, engine
from .logging_config import configure_logging
from .messaging.kafka_producer import ConfluentKafkaProducer, KafkaMessageProducer
from .services.stock_result import (
    IncompatibleStockResultError,
    StockResultCommand,
    StockResultProcessingResult,
    StockResultService,
    parse_stock_result,
)

logger = logging.getLogger(__name__)


class KafkaConsumerClient(Protocol):
    def subscribe(self, topics: list[str]) -> None: ...

    def poll(self, timeout: float): ...

    def commit(self, *, message, asynchronous: bool = False): ...

    def seek(self, partition: TopicPartition) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class IncomingKafkaMessage:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes | None
    headers: dict[str, str | None]


@dataclass(frozen=True, slots=True)
class MessageHandlingResult:
    result: str
    attempts: int
    event_id: str | None


class StockResultMessageHandler:
    def __init__(
        self,
        *,
        result_service: StockResultService,
        dlq_producer: KafkaMessageProducer,
        settings: Settings,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        self._result_service = result_service
        self._dlq_producer = dlq_producer
        self._settings = settings
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._sleeper = sleeper

    def handle(self, message: IncomingKafkaMessage) -> MessageHandlingResult:
        first_failed_at: datetime | None = None
        last_exception: Exception | None = None
        command: StockResultCommand | None = None
        envelope: Mapping[str, Any] | None = None
        category = "TECHNICAL_FAILURE"
        code = "processing_failed"

        for attempt in range(1, self._settings.stock_events_consumer_max_attempts + 1):
            started_at = time.monotonic()
            try:
                envelope = _decode_envelope(message.value)
                command = parse_stock_result(
                    envelope,
                    headers=message.headers,
                    key=message.key,
                )
                processing = self._result_service.process(command)
                self._log_result(
                    command=command,
                    processing=processing,
                    attempt=attempt,
                    duration_ms=(time.monotonic() - started_at) * 1000,
                )
                return MessageHandlingResult(
                    result=processing.result,
                    attempts=attempt,
                    event_id=str(command.event_id),
                )
            except IncompatibleStockResultError as exc:
                category = "INCOMPATIBLE_MESSAGE"
                code = exc.code
                last_exception = exc
            except Exception as exc:
                category = "TECHNICAL_FAILURE"
                code = "processing_failed"
                last_exception = exc
                if command is not None:
                    try:
                        self._result_service.record_retryable_failure(
                            command=command,
                            safe_error="Stock result processing failed",
                            attempt=attempt,
                        )
                    except Exception:
                        logger.warning(
                            "could not persist retryable order inbox state",
                            extra={
                                "consumer": self._settings.stock_events_consumer_name,
                                "event_id": str(command.event_id),
                                "attempt": attempt,
                                "result": "retry_state_unavailable",
                            },
                        )

            failed_at = self._clock()
            first_failed_at = first_failed_at or failed_at
            logger.warning(
                "stock result attempt failed",
                extra={
                    "consumer": self._settings.stock_events_consumer_name,
                    "event_id": str(command.event_id) if command else _event_id(envelope),
                    "event_type": envelope.get("event_type") if envelope else None,
                    "order_id": str(command.order_id) if command else _aggregate_id(envelope),
                    "reservation_request_id": (
                        str(command.reservation_request_id)
                        if command
                        else _reservation_request_id(envelope)
                    ),
                    "supplier_id": (
                        command.supplier_id
                        if command
                        else _supplier_id(envelope)
                    ),
                    "correlation_id": (
                        str(command.correlation_id)
                        if command
                        else _correlation_id(envelope)
                    ),
                    "attempt": attempt,
                    "result": (
                        "retry_scheduled"
                        if attempt < self._settings.stock_events_consumer_max_attempts
                        else "dlq"
                    ),
                    "error_code": code,
                },
            )
            if attempt < self._settings.stock_events_consumer_max_attempts:
                self._sleeper(1 if attempt == 1 else 5)

        failed_at = self._clock()
        dlq_message = _build_dlq_message(
            message=message,
            envelope=envelope,
            category=category,
            code=code,
            safe_message=(
                last_exception.safe_message
                if isinstance(last_exception, IncompatibleStockResultError)
                else "Stock result processing failed"
            ),
            attempt_count=self._settings.stock_events_consumer_max_attempts,
            first_failed_at=first_failed_at or failed_at,
            last_failed_at=failed_at,
            consumer_name=self._settings.stock_events_consumer_name,
        )
        self._dlq_producer.publish(
            topic=self._settings.kafka_dlq_topic,
            key=_aggregate_id(envelope) or message.key or "unknown",
            envelope=dlq_message,
            headers={
                "content_type": "application/json",
                "original_event_id": _event_id(envelope),
                "correlation_id": _correlation_id(envelope),
                "consumer_name": self._settings.stock_events_consumer_name,
            },
        )
        if command is not None:
            self._result_service.mark_terminal_failure(
                command=command,
                safe_error="Stock result moved to DLQ",
                attempt=self._settings.stock_events_consumer_max_attempts,
            )
        logger.error(
            "stock result moved to DLQ",
            extra={
                "consumer": self._settings.stock_events_consumer_name,
                "event_id": _event_id(envelope),
                "event_type": envelope.get("event_type") if envelope else None,
                "order_id": _aggregate_id(envelope),
                "correlation_id": _correlation_id(envelope),
                "attempt": self._settings.stock_events_consumer_max_attempts,
                "topic": self._settings.kafka_dlq_topic,
                "result": "dlq_published",
                "error_code": code,
            },
        )
        return MessageHandlingResult(
            result="DLQ",
            attempts=self._settings.stock_events_consumer_max_attempts,
            event_id=_event_id(envelope),
        )

    def close(self) -> None:
        self._dlq_producer.close()

    def _log_result(
        self,
        *,
        command: StockResultCommand,
        processing: StockResultProcessingResult,
        attempt: int,
        duration_ms: float,
    ) -> None:
        logger.info(
            "stock result processed",
            extra={
                "consumer": self._settings.stock_events_consumer_name,
                "event_id": str(command.event_id),
                "event_type": command.event_type,
                "order_id": str(command.order_id),
                "reservation_request_id": str(command.reservation_request_id),
                "supplier_id": command.supplier_id,
                "correlation_id": str(command.correlation_id),
                "order_version_before": processing.version_before,
                "order_version_after": processing.version_after,
                "transition": (
                    "PENDING_RESERVATION->RESERVED"
                    if processing.result == "RESERVED"
                    else (
                        "PENDING_RESERVATION->REJECTED"
                        if processing.result == "REJECTED"
                        else "NO_STATE_CHANGE"
                    )
                ),
                "result": processing.result,
                "attempt": attempt,
                "duration_ms": round(duration_ms, 3),
            },
        )


class StockEventsConsumerWorker:
    def __init__(
        self,
        *,
        consumer: KafkaConsumerClient,
        handler: StockResultMessageHandler,
        settings: Settings,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._settings = settings
        self._stop_event = threading.Event()

    def run_forever(self) -> None:
        self._consumer.subscribe([self._settings.kafka_stock_events_topic])
        logger.info(
            "order stock events consumer started",
            extra={
                "consumer": self._settings.stock_events_consumer_name,
                "topic": self._settings.kafka_stock_events_topic,
            },
        )
        try:
            while not self._stop_event.is_set():
                message = self._consumer.poll(
                    self._settings.stock_events_consumer_poll_seconds
                )
                if message is None:
                    continue
                if message.error():
                    logger.warning("order stock events consumer poll warning")
                    continue
                try:
                    self.process_message(message)
                except Exception:
                    logger.exception("stock result message was not safely completed")
                    self._consumer.seek(
                        TopicPartition(
                            message.topic(),
                            message.partition(),
                            message.offset(),
                        )
                    )
                    self._stop_event.wait(1)
        finally:
            self._consumer.close()
            self._handler.close()
            logger.info("order stock events consumer stopped")

    def process_message(self, message) -> MessageHandlingResult:
        incoming = IncomingKafkaMessage(
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
            key=(
                None
                if message.key() is None
                else message.key().decode("utf-8", errors="replace")
            ),
            value=message.value(),
            headers=_decode_headers(message.headers()),
        )
        result = self._handler.handle(incoming)
        self._consumer.commit(message=message, asynchronous=False)
        return result

    def stop(self) -> None:
        self._stop_event.set()


def main() -> None:
    settings = get_settings()
    configure_logging(settings)
    if not settings.stock_events_consumer_enabled:
        logger.info("order stock events consumer is disabled")
        return

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.stock_events_consumer_group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "topic.metadata.refresh.interval.ms": 5000,
        }
    )
    dlq_producer = ConfluentKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        delivery_timeout_seconds=settings.kafka_delivery_timeout_seconds,
    )
    result_service = StockResultService(
        session_factory=SessionLocal,
        consumer_name=settings.stock_events_consumer_name,
    )
    handler = StockResultMessageHandler(
        result_service=result_service,
        dlq_producer=dlq_producer,
        settings=settings,
    )
    worker = StockEventsConsumerWorker(
        consumer=consumer,
        handler=handler,
        settings=settings,
    )

    def request_stop(_signum, _frame) -> None:
        logger.info("order stock events consumer shutdown requested")
        worker.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        worker.run_forever()
    finally:
        engine.dispose()


def _decode_envelope(raw_message: bytes | None) -> Mapping[str, Any]:
    if not isinstance(raw_message, bytes):
        raise IncompatibleStockResultError(
            "invalid_json",
            "Kafka message is not valid UTF-8 JSON",
        )
    try:
        decoded = json.loads(raw_message.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IncompatibleStockResultError(
            "invalid_json",
            "Kafka message is not valid UTF-8 JSON",
        ) from exc
    if not isinstance(decoded, Mapping):
        raise IncompatibleStockResultError(
            "invalid_envelope",
            "Kafka message must be an object",
        )
    return decoded


def _decode_headers(
    headers: list[tuple[str, bytes | None]] | None,
) -> dict[str, str | None]:
    return {
        name: (
            None
            if value is None
            else value.decode("utf-8", errors="replace")
        )
        for name, value in (headers or [])
    }


def _build_dlq_message(
    *,
    message: IncomingKafkaMessage,
    envelope: Mapping[str, Any] | None,
    category: str,
    code: str,
    safe_message: str,
    attempt_count: int,
    first_failed_at: datetime,
    last_failed_at: datetime,
    consumer_name: str,
) -> dict[str, Any]:
    original_message: Any
    if envelope is not None:
        original_message = envelope
    else:
        original_message = {
            "raw_utf8": (
                None
                if message.value is None
                else message.value.decode("utf-8", errors="replace")
            ),
        }
    return {
        "dlq_record_id": str(uuid.uuid4()),
        "failed_at": _timestamp(last_failed_at),
        "consumer_name": consumer_name,
        "original_topic": message.topic,
        "original_partition": message.partition,
        "original_offset": message.offset,
        "original_key": message.key,
        "original_headers": message.headers,
        "original_event_id": _event_id(envelope),
        "original_message": original_message,
        "failure": {
            "category": category,
            "code": code,
            "message": safe_message,
            "attempt_count": attempt_count,
            "first_failed_at": _timestamp(first_failed_at),
            "last_failed_at": _timestamp(last_failed_at),
        },
        "correlation_id": _correlation_id(envelope),
    }


def _event_id(envelope: Mapping[str, Any] | None) -> str | None:
    return (
        None
        if envelope is None or envelope.get("event_id") is None
        else str(envelope["event_id"])
    )


def _aggregate_id(envelope: Mapping[str, Any] | None) -> str | None:
    return (
        None
        if envelope is None or envelope.get("aggregate_id") is None
        else str(envelope["aggregate_id"])
    )


def _correlation_id(envelope: Mapping[str, Any] | None) -> str | None:
    return (
        None
        if envelope is None or envelope.get("correlation_id") is None
        else str(envelope["correlation_id"])
    )


def _reservation_request_id(
    envelope: Mapping[str, Any] | None,
) -> str | None:
    payload = None if envelope is None else envelope.get("payload")
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("reservation_request_id")
    return None if value is None else str(value)


def _supplier_id(envelope: Mapping[str, Any] | None) -> int | None:
    payload = None if envelope is None else envelope.get("payload")
    if not isinstance(payload, Mapping):
        return None
    value = payload.get("supplier_id")
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _timestamp(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


if __name__ == "__main__":
    main()
