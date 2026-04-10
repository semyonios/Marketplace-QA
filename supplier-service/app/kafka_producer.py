import json
import os

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SUPPLIER_TOPIC = os.getenv("KAFKA_SUPPLIER_TOPIC", "supplier-events")
PRODUCT_TOPIC = os.getenv("KAFKA_PRODUCT_TOPIC", "product-events")
PRODUCT_STOCK_TOPIC = os.getenv("KAFKA_PRODUCT_STOCK_TOPIC", "product-stock-events")
EVENT_VERSION = 1

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def publish_event(topic: str, key: str, payload: dict) -> None:
    payload.setdefault("event_version", EVENT_VERSION)
    producer.produce(topic, key=key, value=json.dumps(payload, default=str).encode("utf-8"))
    producer.poll(0)



def publish_supplier_event(event_type: str, payload: dict) -> None:
    publish_event(SUPPLIER_TOPIC, str(payload["id"]), {"event_type": event_type, "payload": payload})



def publish_product_event(event_type: str, payload: dict) -> None:
    publish_event(PRODUCT_TOPIC, str(payload["id"]), {"event_type": event_type, "payload": payload})



def publish_product_stock_event(event_type: str, product_id: int, total_quantity: int) -> None:
    publish_event(
        PRODUCT_STOCK_TOPIC,
        str(product_id),
        {
            "event_type": event_type,
            "product_id": product_id,
            "total_quantity": total_quantity,
        },
    )
