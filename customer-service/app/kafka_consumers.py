import json
import logging
import os
import threading
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from confluent_kafka import Consumer
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import Product

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRODUCT_TOPIC = os.getenv("KAFKA_PRODUCT_TOPIC", "product-events")
PRODUCT_STOCK_TOPIC = os.getenv("KAFKA_PRODUCT_STOCK_TOPIC", "product-stock-events")
GROUP_ID = os.getenv("KAFKA_PRODUCTS_GROUP_ID", "customer-product-events-consumer-group")
SUPPLIER_PRODUCTS_URL = os.getenv("SUPPLIER_PRODUCTS_URL", "http://supplier-service:8000/products")
MAX_RETRIES = 3

logger = logging.getLogger(__name__)
_started = False
_lock = threading.Lock()

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 5000,
    }
)



def _parse_created_at(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))



def upsert_product(db: Session, payload: dict) -> bool:
    existing = db.get(Product, payload["id"])
    created_at = _parse_created_at(payload.get("created_at"))
    price = Decimal(str(payload["price"])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    updated_at = datetime.now(timezone.utc)

    if existing and (
        existing.supplier_id == payload.get("supplier_id")
        and existing.name == payload["name"]
        and existing.description == payload.get("description")
        and existing.price == price
        and existing.stocks == payload["stocks"]
        and existing.is_active == payload.get("is_active", True)
        and existing.is_archived == payload.get("is_archived", False)
        and existing.created_at == created_at
    ):
        return False

    values = {
        "id": payload["id"],
        "supplier_id": payload.get("supplier_id"),
        "name": payload["name"],
        "description": payload.get("description"),
        "price": price,
        "stocks": payload["stocks"],
        "is_active": payload.get("is_active", True),
        "is_archived": payload.get("is_archived", False),
        "created_at": created_at,
        "updated_at": updated_at,
    }
    stmt = insert(Product).values(**values)
    db.execute(
        stmt.on_conflict_do_update(
            index_elements=[Product.id],
            set_={
                "name": stmt.excluded.name,
                "supplier_id": stmt.excluded.supplier_id,
                "description": stmt.excluded.description,
                "price": stmt.excluded.price,
                "stocks": stmt.excluded.stocks,
                "is_active": stmt.excluded.is_active,
                "is_archived": stmt.excluded.is_archived,
                "created_at": stmt.excluded.created_at,
                "updated_at": stmt.excluded.updated_at,
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
    product.updated_at = datetime.now(timezone.utc)
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
    logger.info("Customer sync: requesting products from supplier")
    try:
        with urllib.request.urlopen(SUPPLIER_PRODUCTS_URL, timeout=10) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Customer sync warning: failed to fetch products from supplier: %s", exc)
        return

    payload = response_payload["items"] if isinstance(response_payload, dict) and "items" in response_payload else response_payload

    with SessionLocal() as db:
        actual_ids: set[int] = set()
        for item in payload:
            actual_ids.add(item["id"])
            upsert_product(db, item)

        local_products = list(db.query(Product).all())
        for product in local_products:
            if product.id not in actual_ids:
                logger.info("Customer sync: removing stale product %s", product.id)
                if not delete_product(db, product.id):
                    logger.info("Customer sync: skipped stale product %s", product.id)

    logger.info("Customer sync: processed %s products from supplier", len(payload))



def process_message(raw_message: bytes, topic: str) -> None:
    data = json.loads(raw_message.decode("utf-8"))
    event_version = data.get("event_version", 1)
    logger.info("Customer consumer: event received topic=%s event_version=%s payload=%s", topic, event_version, data)
    with SessionLocal() as db:
        if topic == PRODUCT_TOPIC:
            event_type = data["event_type"]
            payload = data["payload"]
            if event_type in {"PRODUCT_CREATED", "PRODUCT_UPDATED"}:
                changed = upsert_product(db, payload)
                logger.info(
                    "Customer consumer: event %s topic=%s product_id=%s",
                    "processed" if changed else "skipped",
                    topic,
                    payload["id"],
                )
            elif event_type == "PRODUCT_DELETED":
                changed = delete_product(db, payload["id"])
                logger.info(
                    "Customer consumer: event %s topic=%s product_id=%s",
                    "processed" if changed else "skipped",
                    topic,
                    payload["id"],
                )
            else:
                logger.info("Customer consumer: event skipped topic=%s reason=unsupported_event_type", topic)
            return

        total_available_stocks = data.get("total_quantity", data.get("stocks"))
        changed = upsert_product_stocks(db, data["product_id"], total_available_stocks)
        if not changed:
            sync_products_from_supplier()
            changed = upsert_product_stocks(db, data["product_id"], total_available_stocks)
        logger.info(
            "Customer consumer: event %s topic=%s product_id=%s",
            "processed" if changed else "skipped",
            topic,
            data["product_id"],
        )



def consume_forever() -> None:
    while True:
        try:
            consumer.subscribe([PRODUCT_TOPIC, PRODUCT_STOCK_TOPIC])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    logger.warning("Customer product consumer warning: %s", message.error())
                    continue

                processed = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        process_message(message.value(), message.topic())
                        consumer.commit(message=message)
                        processed = True
                        break
                    except Exception:
                        logger.exception(
                            "Customer consumer error on attempt %s/%s topic=%s",
                            attempt,
                            MAX_RETRIES,
                            message.topic(),
                        )
                        time.sleep(1)

                if not processed:
                    logger.error("Customer consumer dead letter topic=%s payload=%s", message.topic(), message.value())
        except Exception:
            logger.exception("Customer product consumer error")
            time.sleep(5)



def start_consumers() -> None:
    global _started
    with _lock:
        if _started:
            return
        thread = threading.Thread(target=consume_forever, daemon=True)
        thread.start()
        _started = True
