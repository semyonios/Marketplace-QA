import json
import os
import uuid

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ORDER_TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "order-events")

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})



def publish_event(topic: str, key: str, payload: dict) -> None:
    producer.produce(topic, key=key, value=json.dumps(payload, default=str).encode("utf-8"))
    producer.poll(0)



def publish_order_created(product_id: int, quantity: int) -> None:
    publish_event(
        ORDER_TOPIC,
        str(product_id),
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "ORDER_CREATED",
            "product_id": product_id,
            "quantity": quantity,
        },
    )
