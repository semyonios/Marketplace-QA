import logging
from collections.abc import Generator
from dataclasses import asdict
from functools import partial

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.responses import JSONResponse
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import get_settings
from .database import ReadinessCheckError, SessionLocal, check_readiness
from .error_handlers import register_exception_handlers
from .kafka_producer import publish_product_event, publish_product_stock_event, publish_supplier_event
from .logging_config import configure_logging
from .messaging.kafka import check_kafka_connectivity
from .models import Product, Supplier, Warehouse, WarehouseProduct
from .schemas import (
    ProductCreate,
    ProductListRead,
    ProductRead,
    ProductUpdate,
    RestockRequest,
    SupplierCreate,
    SupplierListRead,
    SupplierRead,
    SupplierUpdate,
    WarehouseCreate,
    WarehouseListRead,
    WarehouseRead,
    WarehouseUpdate,
)

settings = get_settings()
configure_logging(settings)

app = FastAPI(
    title="Сервис поставщика",
    description="API для управления поставщиками, товарами, складами и остатками с публикацией событий в Kafka.",
    version="1.0.0",
    openapi_tags=[
        {"name": "Служебное API", "description": "Технические ручки сервиса и проверка доступности."},
        {"name": "API поставщиков", "description": "Создание, просмотр, обновление и удаление поставщиков."},
        {"name": "API товаров", "description": "Создание, просмотр, обновление и удаление товаров."},
        {"name": "API остатков", "description": "Обновление остатков товаров на складах."},
        {"name": "API складов", "description": "Создание, просмотр, обновление и удаление складов."},
    ],
)

register_exception_handlers(app)
app.state.readiness_checker = partial(
    check_readiness,
    application_settings=settings,
    kafka_checker=partial(
        check_kafka_connectivity,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        timeout_seconds=settings.readiness_timeout_seconds,
    ),
)



def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



def serialize_product(product: Product) -> ProductRead:
    return ProductRead(
        id=product.id,
        supplier_id=product.supplier_id,
        name=product.name,
        description=product.description,
        price=product.price,
        is_active=product.is_active,
        is_archived=product.is_archived,
        stocks=product.stocks,
        total_price=round(product.price * product.stocks, 2),
        created_at=product.created_at,
    )



def recalculate_product_stocks(
    db: Session,
    product_id: int,
    *,
    commit: bool = True,
) -> Product | None:
    product = db.scalar(
        select(Product)
        .where(Product.id == product_id)
        .with_for_update()
    )
    if not product:
        return None

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.product_id == product_id)))
    total_stocks = sum(row.stocks for row in stock_rows)
    if total_stocks < product.reserved_stocks:
        raise HTTPException(status_code=409, detail="stock_below_active_reservation")
    product.stocks = total_stocks
    db.flush()
    if commit:
        db.commit()
        db.refresh(product)
    return product


def ensure_product_state(is_active: bool, is_archived: bool) -> None:
    if is_archived and is_active:
        raise HTTPException(status_code=409, detail="product_archived")


@app.get(
    "/health",
    summary="Проверка доступности",
    description="Возвращает статус доступности `supplier-service`.",
    tags=["Служебное API"],
)
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get(
    "/ready",
    response_model=None,
    summary="Проверка готовности",
    description="Проверяет supplier DB, Alembic revision и Kafka metadata.",
    tags=["Служебное API"],
)
def readiness() -> dict | JSONResponse:
    try:
        state = app.state.readiness_checker()
    except ReadinessCheckError as exc:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": settings.service_name,
                "dependencies": asdict(exc.state),
            },
        )
    return {
        "status": "ready",
        "service": settings.service_name,
        "dependencies": asdict(state),
    }


@app.post(
    "/suppliers",
    response_model=SupplierRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать поставщика",
    description="Создаёт нового поставщика.",
    tags=["API поставщиков"],
)
def create_supplier(supplier_in: SupplierCreate, db: Session = Depends(get_db)) -> Supplier:
    supplier = Supplier(**supplier_in.model_dump())
    db.add(supplier)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="supplier_conflict") from exc

    db.refresh(supplier)
    publish_supplier_event("SUPPLIER_CREATED", SupplierRead.model_validate(supplier).model_dump(mode="json"))
    return supplier


@app.get(
    "/suppliers",
    response_model=SupplierListRead,
    summary="Получить список поставщиков",
    description="Возвращает список всех поставщиков в формате `items + count`.",
    tags=["API поставщиков"],
)
def list_suppliers(db: Session = Depends(get_db)) -> SupplierListRead:
    items = list(db.scalars(select(Supplier).order_by(Supplier.id)))
    return SupplierListRead(items=items, count=len(items))


@app.get(
    "/suppliers/{supplier_id}",
    response_model=SupplierRead,
    summary="Получить поставщика",
    description="Возвращает поставщика по идентификатору.",
    tags=["API поставщиков"],
)
def get_supplier(supplier_id: int, db: Session = Depends(get_db)) -> Supplier:
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier_not_found")
    return supplier


@app.put(
    "/suppliers/{supplier_id}",
    response_model=SupplierRead,
    summary="Обновить поставщика",
    description="Обновляет данные поставщика по идентификатору.",
    tags=["API поставщиков"],
)
def update_supplier(supplier_id: int, supplier_in: SupplierUpdate, db: Session = Depends(get_db)) -> Supplier:
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier_not_found")

    for field, value in supplier_in.model_dump(exclude_unset=True).items():
        setattr(supplier, field, value)

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="supplier_conflict") from exc

    db.refresh(supplier)
    publish_supplier_event("SUPPLIER_UPDATED", SupplierRead.model_validate(supplier).model_dump(mode="json"))
    return supplier


@app.delete(
    "/suppliers/{supplier_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить поставщика",
    description="Удаляет поставщика по идентификатору.",
    tags=["API поставщиков"],
)
def delete_supplier(supplier_id: int, db: Session = Depends(get_db)) -> Response:
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier_not_found")

    has_products = db.scalar(select(Product.id).where(Product.supplier_id == supplier_id).limit(1))
    if has_products is not None:
        raise HTTPException(status_code=409, detail="supplier_has_products")

    payload = SupplierRead.model_validate(supplier).model_dump(mode="json")
    db.delete(supplier)
    db.commit()
    publish_supplier_event("SUPPLIER_DELETED", payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/warehouses",
    response_model=WarehouseRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать склад",
    description="Создаёт склад с названием, графиком работы и адресом.",
    tags=["API складов"],
)
def create_warehouse(warehouse_in: WarehouseCreate, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = Warehouse(**warehouse_in.model_dump())
    db.add(warehouse)
    db.commit()
    db.refresh(warehouse)
    return warehouse


@app.get(
    "/warehouses",
    response_model=WarehouseListRead,
    summary="Получить список складов",
    description="Возвращает список всех складов в формате `items + count`.",
    tags=["API складов"],
)
def list_warehouses(db: Session = Depends(get_db)) -> WarehouseListRead:
    items = list(db.scalars(select(Warehouse).order_by(Warehouse.id)))
    return WarehouseListRead(items=items, count=len(items))


@app.get(
    "/warehouses/{warehouse_id}",
    response_model=WarehouseRead,
    summary="Получить склад",
    description="Возвращает склад по идентификатору.",
    tags=["API складов"],
)
def get_warehouse(warehouse_id: int, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")
    return warehouse


@app.put(
    "/warehouses/{warehouse_id}",
    response_model=WarehouseRead,
    summary="Обновить склад",
    description="Обновляет название, график работы и адрес склада.",
    tags=["API складов"],
)
def update_warehouse(warehouse_id: int, warehouse_in: WarehouseUpdate, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")

    for field, value in warehouse_in.model_dump(exclude_unset=True).items():
        setattr(warehouse, field, value)

    db.commit()
    db.refresh(warehouse)
    return warehouse


@app.delete(
    "/warehouses/{warehouse_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить склад",
    description="Удаляет склад и пересчитывает агрегированные остатки затронутых товаров.",
    tags=["API складов"],
)
def delete_warehouse(warehouse_id: int, db: Session = Depends(get_db)) -> Response:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.warehouse_id == warehouse_id)))
    affected_product_ids = sorted({row.product_id for row in stock_rows})
    db.execute(delete(WarehouseProduct).where(WarehouseProduct.warehouse_id == warehouse_id))
    db.delete(warehouse)
    recalculated_products: list[Product] = []
    for product_id in affected_product_ids:
        product = recalculate_product_stocks(db, product_id, commit=False)
        if product:
            recalculated_products.append(product)
    db.commit()

    for product in recalculated_products:
        publish_product_stock_event("STOCK_REPLENISHED", product.id, product.stocks)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/products",
    response_model=ProductRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать товар",
    description="Создаёт товар для поставщика. Остатки управляются отдельно через склады.",
    tags=["API товаров"],
)
def create_product(product_in: ProductCreate, db: Session = Depends(get_db)) -> ProductRead:
    supplier = db.get(Supplier, product_in.supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier_not_found")

    ensure_product_state(product_in.is_active, product_in.is_archived)

    product = Product(**product_in.model_dump(), stocks=0)
    db.add(product)
    db.commit()
    db.refresh(product)

    payload = serialize_product(product).model_dump(mode="json")
    publish_product_event("PRODUCT_CREATED", payload)
    return serialize_product(product)


@app.get(
    "/products",
    response_model=ProductListRead,
    summary="Получить список товаров",
    description="Возвращает список всех товаров в формате `items + count`.",
    tags=["API товаров"],
)
def list_products(db: Session = Depends(get_db)) -> ProductListRead:
    items = [serialize_product(product) for product in db.scalars(select(Product).order_by(Product.id))]
    return ProductListRead(items=items, count=len(items))


@app.get(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Получить товар",
    description="Возвращает товар по идентификатору вместе с агрегированным остатком.",
    tags=["API товаров"],
)
def get_product(product_id: int, db: Session = Depends(get_db)) -> ProductRead:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")
    return serialize_product(product)


@app.put(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Обновить товар",
    description="Обновляет поля товара без изменения складских строк остатков.",
    tags=["API товаров"],
)
def update_product(product_id: int, product_in: ProductUpdate, db: Session = Depends(get_db)) -> ProductRead:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")

    updates = product_in.model_dump(exclude_unset=True)
    supplier_id = updates.get("supplier_id")
    if supplier_id is not None and not db.get(Supplier, supplier_id):
        raise HTTPException(status_code=404, detail="supplier_not_found")

    next_is_active = updates.get("is_active", product.is_active)
    next_is_archived = updates.get("is_archived", product.is_archived)
    ensure_product_state(next_is_active, next_is_archived)

    for field, value in updates.items():
        setattr(product, field, value)

    db.commit()
    db.refresh(product)

    payload = serialize_product(product).model_dump(mode="json")
    publish_product_event("PRODUCT_UPDATED", payload)
    return serialize_product(product)


@app.post(
    "/warehouses/{warehouse_id}/stocks",
    response_model=ProductListRead,
    summary="Обновить остатки на складе",
    description="Обновляет фактические остатки товаров на складе и публикует агрегированные события по остаткам.",
    tags=["API остатков"],
)
def restock_products(warehouse_id: int, restock_in: RestockRequest, db: Session = Depends(get_db)) -> ProductListRead:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")

    seen_product_ids: set[int] = set()

    for item in restock_in.items:
        product = db.get(Product, item.product_id)
        if not product:
            raise HTTPException(status_code=404, detail="product_not_found")

        stock_row = db.scalar(
            select(WarehouseProduct).where(
                WarehouseProduct.warehouse_id == warehouse_id,
                WarehouseProduct.product_id == item.product_id,
            )
        )
        if stock_row:
            stock_row.stocks = item.stocks
        else:
            db.add(WarehouseProduct(warehouse_id=warehouse_id, product_id=item.product_id, stocks=item.stocks))
        seen_product_ids.add(item.product_id)

    db.flush()
    recalculated_products = [
        product
        for product_id in sorted(seen_product_ids)
        if (product := recalculate_product_stocks(db, product_id, commit=False)) is not None
    ]
    db.commit()
    updated_products = [serialize_product(product) for product in recalculated_products]
    for product in recalculated_products:
        publish_product_stock_event("STOCK_REPLENISHED", product.id, product.stocks)

    return ProductListRead(items=updated_products, count=len(updated_products))


@app.delete(
    "/products/{product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить товар",
    description="Архивирует товар по идентификатору.",
    tags=["API товаров"],
)
def delete_product(product_id: int, db: Session = Depends(get_db)) -> Response:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")

    product.is_active = False
    product.is_archived = True
    db.commit()
    db.refresh(product)

    payload = serialize_product(product).model_dump(mode="json")
    publish_product_event("PRODUCT_UPDATED", payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
