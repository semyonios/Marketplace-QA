import json
import os
import time

from confluent_kafka import Consumer

from .database import Base, SessionLocal, engine
from .models import UserEvent

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user-events")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "audit-consumer-group")

Base.metadata.create_all(bind=engine)

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)


def process_message(raw_message: bytes) -> None:
    data = json.loads(raw_message.decode("utf-8"))
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

def main() -> None:
    while True:
        try:
            consumer.subscribe([TOPIC])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    raise RuntimeError(message.error())
                process_message(message.value())
        except Exception as exc:
            print(f"Consumer error: {exc}")
            time.sleep(5)


if __name__ == "__main__":
    main()
