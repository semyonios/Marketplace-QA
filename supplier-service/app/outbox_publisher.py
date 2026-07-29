from __future__ import annotations

import logging
import os
import signal
import socket

from .config import get_settings
from .database import SessionLocal, engine
from .logging_config import configure_logging
from .messaging.kafka import ConfluentJsonProducer
from .services.outbox_publisher import (
    SupplierOutboxPublisher,
    SupplierOutboxPublisherConfig,
    SupplierOutboxPublisherWorker,
)

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings)
    if not settings.supplier_outbox_publisher_enabled:
        logger.info("supplier outbox publisher is disabled")
        return

    producer = ConfluentJsonProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        delivery_timeout_seconds=settings.kafka_delivery_timeout_seconds,
    )
    publisher = SupplierOutboxPublisher(
        session_factory=SessionLocal,
        producer=producer,
        config=SupplierOutboxPublisherConfig(
            batch_size=settings.outbox_batch_size,
            claim_lease_seconds=settings.outbox_claim_lease_seconds,
            base_retry_delay_seconds=settings.outbox_base_retry_delay_seconds,
            max_retry_delay_seconds=settings.outbox_max_retry_delay_seconds,
            max_attempts=settings.outbox_max_attempts,
            topics_by_event_type={
                "StockReservationSucceeded": settings.stock_events_topic,
                "StockReservationFailed": settings.stock_events_topic,
                "StockFinalized": settings.stock_events_topic,
                "StockFinalizationFailed": settings.stock_events_topic,
                "StockReleased": settings.stock_events_topic,
                "StockReleaseFailed": settings.stock_events_topic,
            },
        ),
        worker_id=f"{socket.gethostname()}-{os.getpid()}",
    )
    worker = SupplierOutboxPublisherWorker(
        publisher=publisher,
        poll_interval_seconds=settings.outbox_poll_interval_seconds,
    )

    def request_stop(_signum, _frame) -> None:
        logger.info("supplier outbox shutdown requested")
        worker.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        worker.run_forever()
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
