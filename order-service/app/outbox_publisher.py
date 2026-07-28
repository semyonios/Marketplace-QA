from __future__ import annotations

import logging
import os
import signal
import socket

from .config import get_settings
from .database import SessionLocal, engine
from .logging_config import configure_logging
from .messaging.kafka_producer import ConfluentKafkaProducer
from .services.outbox_publisher import (
    OutboxPublisher,
    OutboxPublisherConfig,
    OutboxPublisherWorker,
)

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings)
    if not settings.outbox_publisher_enabled:
        logger.info("outbox publisher is disabled")
        return

    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    producer = ConfluentKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        delivery_timeout_seconds=settings.kafka_delivery_timeout_seconds,
    )
    publisher = OutboxPublisher(
        session_factory=SessionLocal,
        producer=producer,
        config=OutboxPublisherConfig(
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
            batch_size=settings.outbox_batch_size,
            claim_lease_seconds=settings.outbox_claim_lease_seconds,
            base_retry_delay_seconds=settings.outbox_base_retry_delay_seconds,
            max_retry_delay_seconds=settings.outbox_max_retry_delay_seconds,
            max_attempts=settings.outbox_max_attempts,
            topics_by_event_type={
                "OrderCreated": settings.kafka_order_events_topic,
                "StockReservationRequested": settings.kafka_stock_commands_topic,
            },
        ),
        worker_id=worker_id,
    )
    worker = OutboxPublisherWorker(
        publisher=publisher,
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
    )

    def request_stop(_signum, _frame) -> None:
        logger.info("outbox publisher shutdown requested")
        worker.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        worker.run_forever()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
