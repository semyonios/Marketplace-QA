import json
import os
import threading
import time
import urllib.request
from datetime import datetime

from confluent_kafka import Consumer
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import Product

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRODUCT_TOPIC = os.getenv("KAFKA_PRODUCT_TOPIC", "products-events")
STOCK_SUPPLIER_TOPIC = os.getenv("KAFKA_STOCK_SUPPLIER_TOPIC", "stock-supplier-events")
GROUP_ID = os.getenv("KAFKA_PRODUCTS_GROUP_ID", "customer-products-consumer-group")
SUPPLIER_PRODUCTS_URL = os.getenv("SUPPLIER_PRODUCTS_URL", "http://user-service:8000/products")

_started = False
_lock = threading.Lock()

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)



def _parse_created_at(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))



def upsert_product(db: Session, payload: dict) -> bool:
    existing = db.get(Product, payload["id"])
    created_at = _parse_created_at(payload.get("created_at"))

    if existing and (
        existing.name == payload["name"]
        and existing.description == payload.get("description")
        and existing.price == payload["price"]
        and existing.stocks == payload["stocks"]
        and existing.created_at == created_at
    ):
        return False

    values = {
        "id": payload["id"],
        "name": payload["name"],
        "description": payload.get("description"),
        "price": payload["price"],
        "stocks": payload["stocks"],
        "created_at": created_at,
    }
    stmt = insert(Product).values(**values)
    db.execute(
        stmt.on_conflict_do_update(
            index_elements=[Product.id],
            set_={
                "name": stmt.excluded.name,
                "description": stmt.excluded.description,
                "price": stmt.excluded.price,
                "stocks": stmt.excluded.stocks,
                "created_at": stmt.excluded.created_at,
            },
        )
    )
    db.commit()
    return True



def upsert_product_stocks(db: Session, product_id: int, total_quantity: int) -> bool:
    product = db.get(Product, product_id)
    if not product:
        return False
    if product.stocks == total_quantity:
        return False
    product.stocks = total_quantity
    db.commit()
    return True



def delete_product(db: Session, product_id: int) -> bool:
    product = db.get(Product, product_id)
    if not product:
        return False
    try:
        db.delete(product)
        db.commit()
    except IntegrityError:
        db.rollback()
        return False
    return True



def sync_products_from_supplier() -> None:
    print("Customer sync: requesting products from supplier")
    try:
        with urllib.request.urlopen(SUPPLIER_PRODUCTS_URL, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        print(f"Customer sync warning: failed to fetch products from supplier: {exc}")
        return

    with SessionLocal() as db:
        actual_ids: set[int] = set()
        for item in payload:
            actual_ids.add(item["id"])
            upsert_product(db, item)

        local_products = list(db.query(Product).all())
        for product in local_products:
            if product.id not in actual_ids:
                print(f"Customer sync: removing stale product {product.id}")
                if not delete_product(db, product.id):
                    print(f"Customer sync: skipped stale product {product.id}")

    print(f"Customer sync: processed {len(payload)} products from supplier")



def process_message(raw_message: bytes, topic: str) -> None:
    data = json.loads(raw_message.decode("utf-8"))
    print(f"Customer consumer: event received topic={topic} payload={data}")
    with SessionLocal() as db:
        if topic == PRODUCT_TOPIC:
            event_type = data["event_type"]
            payload = data["payload"]
            if event_type in {"PRODUCT_CREATED", "PRODUCT_UPDATED"}:
                changed = upsert_product(db, payload)
                print(
                    f"Customer consumer: event {'processed' if changed else 'skipped'} topic={topic} product_id={payload['id']}"
                )
            elif event_type == "PRODUCT_DELETED":
                changed = delete_product(db, payload["id"])
                print(
                    f"Customer consumer: event {'processed' if changed else 'skipped'} topic={topic} product_id={payload['id']}"
                )
            return

        total_quantity = data.get("total_quantity", data.get("stocks"))
        changed = upsert_product_stocks(db, data["product_id"], total_quantity)
        print(
            f"Customer consumer: event {'processed' if changed else 'skipped'} topic={topic} product_id={data['product_id']}"
        )



def consume_forever() -> None:
    while True:
        try:
            consumer.subscribe([PRODUCT_TOPIC, STOCK_SUPPLIER_TOPIC])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    print(f"Customer product consumer warning: {message.error()}")
                    continue
                process_message(message.value(), message.topic())
        except Exception as exc:
            print(f"Customer product consumer error: {exc}")
            time.sleep(5)



def start_consumers() -> None:
    global _started
    with _lock:
        if _started:
            return
        thread = threading.Thread(target=consume_forever, daemon=True)
        thread.start()
        _started = True
