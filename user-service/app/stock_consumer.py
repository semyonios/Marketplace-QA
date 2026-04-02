import json
import os
import threading
import time

from confluent_kafka import Consumer
from sqlalchemy import select
from sqlalchemy.orm import Session

from .database import SessionLocal
from .kafka_producer import publish_supplier_stock_event
from .models import Product, WarehouseProduct

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "order-events")
GROUP_ID = os.getenv("KAFKA_ORDER_GROUP_ID", "supplier-order-consumer-group")

_started = False
_lock = threading.Lock()
_processed_event_ids: set[str] = set()

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)



def apply_order(db: Session, product_id: int, order_quantity: int) -> bool:
    product = db.get(Product, product_id)
    if not product:
        return False

    rows = list(
        db.scalars(
            select(WarehouseProduct)
            .where(WarehouseProduct.product_id == product_id)
            .order_by(WarehouseProduct.warehouse_id, WarehouseProduct.product_id)
        )
    )

    if not rows:
        return False

    remaining = order_quantity
    initial_stocks = product.stocks
    for row in rows:
        if remaining <= 0:
            break
        taken = min(row.stocks, remaining)
        row.stocks -= taken
        remaining -= taken

    db.commit()

    fresh_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.product_id == product_id)))
    product.stocks = sum(row.stocks for row in fresh_rows)
    db.commit()
    db.refresh(product)

    if product.stocks == initial_stocks:
        return False

    publish_supplier_stock_event("STOCK_DECREASED_BY_ORDER", product.id, product.stocks)
    return True



def consume_forever() -> None:
    while True:
        try:
            consumer.subscribe([TOPIC])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f"Supplier order consumer warning: {message.error()}")
                    continue

                data = json.loads(message.value().decode("utf-8"))
                print(f"Supplier consumer: event received topic={TOPIC} payload={data}")
                if data.get("event_type") != "ORDER_CREATED":
                    print("Supplier consumer: event skipped topic=order-events reason=unsupported_event_type")
                    continue

                event_id = data.get("event_id")
                if event_id and event_id in _processed_event_ids:
                    print(f"Supplier consumer: event skipped topic=order-events event_id={event_id}")
                    continue

                with SessionLocal() as db:
                    changed = apply_order(db, data["product_id"], data["quantity"])

                if event_id:
                    _processed_event_ids.add(event_id)
                print(
                    f"Supplier consumer: event {'processed' if changed else 'skipped'} topic=order-events product_id={data['product_id']}"
                )
        except Exception as exc:
            print(f"Supplier order consumer error: {exc}")
            time.sleep(5)



def start_stock_consumer() -> None:
    global _started
    with _lock:
        if _started:
            return
        thread = threading.Thread(target=consume_forever, daemon=True)
        thread.start()
        _started = True
