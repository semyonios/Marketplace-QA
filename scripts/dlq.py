#!/usr/bin/env python3
"""Inspect and manually replay Marketplace-QA DLQ records."""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:9092")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "marketplace.order.dlq.v1")


def records(limit: int) -> list[dict[str, Any]]:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"marketplace-dlq-inspect-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([DLQ_TOPIC])
    result: list[dict[str, Any]] = []
    idle_polls = 0
    try:
        while len(result) < limit and idle_polls < 3:
            message = consumer.poll(1.0)
            if message is None:
                idle_polls += 1
                continue
            if message.error():
                if message.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    return result
                raise RuntimeError(str(message.error()))
            idle_polls = 0
            payload = json.loads(message.value().decode("utf-8"))
            payload["_kafka"] = {
                "partition": message.partition(),
                "offset": message.offset(),
            }
            result.append(payload)
    finally:
        consumer.close()
    return result


def list_records(limit: int) -> None:
    found = records(limit)
    if not found:
        print("DLQ is empty")
        return
    for record in found:
        print(
            json.dumps(
                {
                    "dlq_record_id": record.get("dlq_record_id"),
                    "original_event_id": record.get("original_event_id"),
                    "original_topic": record.get("original_topic"),
                    "original_key": record.get("original_key"),
                    "consumer_name": record.get("consumer_name"),
                    "failure": record.get("failure"),
                    "correlation_id": record.get("correlation_id"),
                    "kafka": record.get("_kafka"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )


def replay(record_id: str, limit: int, confirmed: bool) -> None:
    if not confirmed:
        raise SystemExit(
            "Replay is manual and may redeliver a business event. "
            "Re-run with --yes after checking inbox/idempotency state."
        )
    record = next(
        (
            candidate
            for candidate in records(limit)
            if candidate.get("dlq_record_id") == record_id
        ),
        None,
    )
    if record is None:
        raise SystemExit(f"DLQ record not found within first {limit}: {record_id}")
    original = record.get("original_message")
    topic = record.get("original_topic")
    if not isinstance(original, dict) or not isinstance(topic, str):
        raise SystemExit("Record has no replayable original_message/original_topic")

    if set(original) == {"raw_utf8"}:
        raw_value = original["raw_utf8"]
        if not isinstance(raw_value, str):
            raise SystemExit("Record has no replayable UTF-8 original message")
        serialized_value = raw_value.encode("utf-8")
    else:
        serialized_value = json.dumps(original).encode("utf-8")

    raw_headers = record.get("original_headers") or {}
    headers = [
        (str(key), None if value is None else str(value).encode("utf-8"))
        for key, value in raw_headers.items()
    ]
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    delivery_error: list[str] = []

    def delivered(error, _message) -> None:
        if error is not None:
            delivery_error.append(str(error))

    producer.produce(
        topic,
        key=(
            None
            if record.get("original_key") is None
            else str(record["original_key"]).encode("utf-8")
        ),
        value=serialized_value,
        headers=headers,
        on_delivery=delivered,
    )
    remaining = producer.flush(10)
    if remaining or delivery_error:
        raise SystemExit(
            f"Replay delivery failed: remaining={remaining}, errors={delivery_error}"
        )
    print(
        f"Replayed {record_id} to {topic}; original event_id was preserved. "
        "Verify consumer inbox before any further replay."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    list_parser = subparsers.add_parser("list")
    list_parser.add_argument("--limit", type=int, default=20)
    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("record_id")
    replay_parser.add_argument("--limit", type=int, default=1000)
    replay_parser.add_argument("--yes", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.command == "list":
        list_records(arguments.limit)
    else:
        replay(arguments.record_id, arguments.limit, arguments.yes)
