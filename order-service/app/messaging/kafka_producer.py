from __future__ import annotations

import json
import threading
import time
from collections.abc import Mapping
from typing import Any, Protocol

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient


class KafkaDeliveryError(RuntimeError):
    """Raised when Kafka does not acknowledge delivery within the configured timeout."""


class KafkaMessageProducer(Protocol):
    def publish(
        self,
        *,
        topic: str,
        key: str,
        envelope: Mapping[str, Any],
        headers: Mapping[str, Any],
    ) -> None: ...

    def close(self) -> None: ...


class ConfluentKafkaProducer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        delivery_timeout_seconds: float,
    ) -> None:
        self._delivery_timeout_seconds = delivery_timeout_seconds
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 2_147_483_647,
                "delivery.timeout.ms": int(delivery_timeout_seconds * 1000),
                "request.timeout.ms": max(1000, int(delivery_timeout_seconds * 1000)),
            }
        )

    def publish(
        self,
        *,
        topic: str,
        key: str,
        envelope: Mapping[str, Any],
        headers: Mapping[str, Any],
    ) -> None:
        acknowledgement = threading.Event()
        delivery_error: list[Exception | None] = [None]

        def on_delivery(error, _message) -> None:
            if error is not None:
                delivery_error[0] = KafkaDeliveryError(str(error))
            acknowledgement.set()

        serialized_headers = [
            (name, None if value is None else str(value).encode("utf-8"))
            for name, value in headers.items()
        ]
        self._producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(
                envelope,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8"),
            headers=serialized_headers,
            on_delivery=on_delivery,
        )
        deadline = time.monotonic() + self._delivery_timeout_seconds
        while not acknowledgement.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise KafkaDeliveryError("Kafka delivery acknowledgement timed out")
            self._producer.poll(min(0.1, remaining))

        if delivery_error[0] is not None:
            raise delivery_error[0]

    def close(self) -> None:
        self._producer.flush(self._delivery_timeout_seconds)


def check_kafka_connectivity(*, bootstrap_servers: str, timeout_seconds: float) -> None:
    client = AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
            "socket.timeout.ms": max(1000, int(timeout_seconds * 1000)),
        }
    )
    client.list_topics(timeout=timeout_seconds)
