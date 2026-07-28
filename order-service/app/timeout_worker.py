from __future__ import annotations

import logging
import signal
import threading

from .config import get_settings
from .database import SessionLocal, engine
from .logging_config import configure_logging
from .services.operation_timeout import OperationTimeoutConfig, OperationTimeoutService

logger = logging.getLogger(__name__)


def main() -> None:
    settings = get_settings()
    configure_logging(settings)
    if not settings.timeout_worker_enabled:
        logger.info("order timeout worker is disabled")
        return

    stop_event = threading.Event()
    service = OperationTimeoutService(
        session_factory=SessionLocal,
        config=OperationTimeoutConfig(
            batch_size=settings.timeout_worker_batch_size,
            retry_seconds=settings.timeout_worker_retry_seconds,
            max_attempts=settings.timeout_worker_max_attempts,
        ),
    )

    def request_stop(_signum, _frame) -> None:
        logger.info("order timeout worker shutdown requested")
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    logger.info("order timeout worker started")
    try:
        while not stop_event.is_set():
            try:
                service.process_once()
            except Exception:
                logger.exception("order timeout worker polling failed")
            stop_event.wait(settings.timeout_worker_poll_seconds)
    finally:
        engine.dispose()
        logger.info("order timeout worker stopped")


if __name__ == "__main__":
    main()
