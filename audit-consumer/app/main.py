import json
import logging
import os
import time

from confluent_kafka import Consumer

from .database import Base, SessionLocal, engine
from .models import UserEvent

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "audit-consumer-group")
MAX_RETRIES = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base.metadata.create_all(bind=engine)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 5000,
    }
)



def process_message(raw_message: bytes) -> None:
    data = json.loads(raw_message.decode("utf-8"))
    event_version = data.get("event_version", 1)
    logger.info("Audit consumer: event received topic=%s event_version=%s payload=%s", TOPIC, event_version, data)
    payload = data["payload"]

    with SessionLocal() as db:
        db.add(
            UserEvent(
                event_type=data["event_type"],
                user_id=payload["id"],
                payload=payload,
            )
        )
        db.commit()

    logger.info("Audit consumer: event processed topic=%s user_id=%s", TOPIC, payload["id"])



def main() -> None:
    while True:
        try:
            consumer.subscribe([TOPIC])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    logger.warning("Audit consumer warning: %s", message.error())
                    continue

                processed = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        process_message(message.value())
                        consumer.commit(message=message)
                        processed = True
                        break
                    except Exception:
                        logger.exception("Audit consumer error on attempt %s/%s topic=%s", attempt, MAX_RETRIES, TOPIC)
                        time.sleep(1)

                if not processed:
                    logger.error("Audit consumer dead letter topic=%s payload=%s", TOPIC, message.value())
        except Exception:
            logger.exception("Audit consumer main loop error")
            time.sleep(5)


if __name__ == "__main__":
    main()
