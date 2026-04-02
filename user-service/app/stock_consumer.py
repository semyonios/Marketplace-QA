import json
import logging
import os
import threading
import time
import uuid

from confluent_kafka import Consumer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from .database import SessionLocal
from .kafka_producer import publish_supplier_stock_event
from .models import ProcessedEvent, Product, WarehouseProduct

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "order-events")
GROUP_ID = os.getenv("KAFKA_ORDER_GROUP_ID", "supplier-order-consumer-group")
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



def reserve_event_id(db: Session, event_id: str | None) -> bool:
    if not event_id:
        return True

    stmt = (
        insert(ProcessedEvent)
        .values(event_id=uuid.UUID(event_id))
        .on_conflict_do_nothing(index_elements=[ProcessedEvent.event_id])
        .returning(ProcessedEvent.event_id)
    )
    inserted_event_id = db.execute(stmt).scalar_one_or_none()
    return inserted_event_id is not None



def apply_order(db: Session, product_id: int, order_quantity: int) -> tuple[bool, int] | tuple[bool, None]:
    product = db.get(Product, product_id)
    if not product:
        return False, None

    rows = list(
        db.scalars(
            select(WarehouseProduct)
            .where(WarehouseProduct.product_id == product_id)
            .order_by(WarehouseProduct.warehouse_id, WarehouseProduct.product_id)
        )
    )
    if not rows:
        return False, None

    remaining = order_quantity
    initial_stocks = product.stocks
    for row in rows:
        if remaining <= 0:
            break
        taken = min(row.stocks, remaining)
        row.stocks -= taken
        remaining -= taken

    fresh_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.product_id == product_id)))
    product.stocks = sum(row.stocks for row in fresh_rows)
    db.flush()

    if product.stocks == initial_stocks:
        return False, product.stocks

    return True, product.stocks



def process_message(raw_message: bytes) -> tuple[bool, str]:
    data = json.loads(raw_message.decode("utf-8"))
    event_version = data.get("event_version", 1)
    logger.info("event received topic=%s event_version=%s payload=%s", TOPIC, event_version, data)

    if data.get("event_type") != "ORDER_CREATED":
        logger.info("event skipped topic=%s reason=unsupported_event_type", TOPIC)
        return False, "skipped"

    with SessionLocal() as db:
        inserted = reserve_event_id(db, data.get("event_id"))
        if not inserted:
            db.rollback()
            logger.info("event skipped topic=%s event_id=%s", TOPIC, data.get("event_id"))
            return False, "skipped"

        changed, total_quantity = apply_order(db, data["product_id"], data["quantity"])
        db.commit()

    if changed and total_quantity is not None:
        publish_supplier_stock_event("STOCK_DECREASED_BY_ORDER", data["product_id"], total_quantity)

    logger.info(
        "event %s topic=%s product_id=%s",
        "processed" if changed else "skipped",
        TOPIC,
        data["product_id"],
    )
    return changed, "processed" if changed else "skipped"



def consume_forever() -> None:
    while True:
        try:
            consumer.subscribe([TOPIC])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    logger.warning("Supplier order consumer warning: %s", message.error())
                    continue

                processed = False
                for attempt in range(1, MAX_RETRIES + 1):
                    try:
                        process_message(message.value())
                        consumer.commit(message=message)
                        processed = True
                        break
                    except (ValueError, SQLAlchemyError, json.JSONDecodeError):
                        logger.exception(
                            "Supplier consumer error on attempt %s/%s topic=%s", attempt, MAX_RETRIES, TOPIC
                        )
                        time.sleep(1)
                    except Exception:
                        logger.exception(
                            "Supplier consumer unexpected error on attempt %s/%s topic=%s",
                            attempt,
                            MAX_RETRIES,
                            TOPIC,
                        )
                        time.sleep(1)

                if not processed:
                    logger.error("Supplier consumer dead letter topic=%s payload=%s", TOPIC, message.value())
        except Exception:
            logger.exception("Supplier order consumer error")
            time.sleep(5)



def start_stock_consumer() -> None:
    global _started
    with _lock:
        if _started:
            return
        thread = threading.Thread(target=consume_forever, daemon=True)
        thread.start()
        _started = True
