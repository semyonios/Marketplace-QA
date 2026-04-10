import logging
from collections.abc import Generator

from fastapi import Depends, FastAPI, HTTPException, Response, status
from sqlalchemy import delete, inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine
from .error_handlers import register_exception_handlers
from .kafka_producer import publish_product_event, publish_product_stock_event, publish_supplier_event
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
from .stock_consumer import start_stock_consumer

logging.basicConfig(level=logging.INFO)


def ensure_supplier_schema() -> None:
    Base.metadata.create_all(bind=engine)

    inspector = inspect(engine)
    if "products" not in inspector.get_table_names():
        return

    product_columns = {column["name"] for column in inspector.get_columns("products")}
    with engine.begin() as connection:
        if "is_active" not in product_columns:
            connection.execute(text("ALTER TABLE products ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE"))
        if "is_archived" not in product_columns:
            connection.execute(text("ALTER TABLE products ADD COLUMN is_archived BOOLEAN NOT NULL DEFAULT FALSE"))

app = FastAPI(
    title="Supplier Service",
    description="Supplier-side management for suppliers, products, warehouses, and stock publishing to Kafka.",
    version="1.0.0",
    openapi_tags=[
        {"name": "Service API", "description": "Technical and health endpoints."},
        {"name": "Suppliers API", "description": "Create, read, update, and delete suppliers."},
        {"name": "Products API", "description": "Create, read, update, and delete products."},
        {"name": "Stock API", "description": "Update product stock in warehouses."},
        {"name": "Warehouses API", "description": "Create, read, update, and delete warehouses."},
    ],
)

register_exception_handlers(app)


@app.on_event("startup")
def startup() -> None:
    ensure_supplier_schema()
    start_stock_consumer()



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



def recalculate_product_stocks(db: Session, product_id: int) -> Product | None:
    product = db.get(Product, product_id)
    if not product:
        return None

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.product_id == product_id)))
    product.stocks = sum(row.stocks for row in stock_rows)
    db.commit()
    db.refresh(product)
    return product


def ensure_product_state(is_active: bool, is_archived: bool) -> None:
    if is_archived and is_active:
        raise HTTPException(status_code=409, detail="product_archived")


@app.get(
    "/health",
    summary="Healthcheck",
    description="Returns supplier-service availability status.",
    tags=["Service API"],
)
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/suppliers",
    response_model=SupplierRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create supplier",
    description="Creates a supplier profile in supplier-service.",
    tags=["Suppliers API"],
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
    summary="List suppliers",
    description="Returns all suppliers in a QA-friendly list wrapper.",
    tags=["Suppliers API"],
)
def list_suppliers(db: Session = Depends(get_db)) -> SupplierListRead:
    items = list(db.scalars(select(Supplier).order_by(Supplier.id)))
    return SupplierListRead(items=items, count=len(items))


@app.get(
    "/suppliers/{supplier_id}",
    response_model=SupplierRead,
    summary="Get supplier",
    description="Returns a supplier by ID.",
    tags=["Suppliers API"],
)
def get_supplier(supplier_id: int, db: Session = Depends(get_db)) -> Supplier:
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier_not_found")
    return supplier


@app.put(
    "/suppliers/{supplier_id}",
    response_model=SupplierRead,
    summary="Update supplier",
    description="Updates supplier profile data by ID.",
    tags=["Suppliers API"],
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
    summary="Delete supplier",
    description="Deletes a supplier by ID.",
    tags=["Suppliers API"],
)
def delete_supplier(supplier_id: int, db: Session = Depends(get_db)) -> Response:
    supplier = db.get(Supplier, supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier_not_found")

    payload = SupplierRead.model_validate(supplier).model_dump(mode="json")
    db.delete(supplier)
    db.commit()
    publish_supplier_event("SUPPLIER_DELETED", payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/warehouses",
    response_model=WarehouseRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create warehouse",
    description="Creates a warehouse with name, weekday schedule, and address.",
    tags=["Warehouses API"],
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
    summary="List warehouses",
    description="Returns all warehouses in a QA-friendly list wrapper.",
    tags=["Warehouses API"],
)
def list_warehouses(db: Session = Depends(get_db)) -> WarehouseListRead:
    items = list(db.scalars(select(Warehouse).order_by(Warehouse.id)))
    return WarehouseListRead(items=items, count=len(items))


@app.get(
    "/warehouses/{warehouse_id}",
    response_model=WarehouseRead,
    summary="Get warehouse",
    description="Returns a warehouse by ID.",
    tags=["Warehouses API"],
)
def get_warehouse(warehouse_id: int, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")
    return warehouse


@app.put(
    "/warehouses/{warehouse_id}",
    response_model=WarehouseRead,
    summary="Update warehouse",
    description="Updates warehouse name, weekday schedule, and address.",
    tags=["Warehouses API"],
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
    summary="Delete warehouse",
    description="Deletes a warehouse and recalculates affected product stock totals.",
    tags=["Warehouses API"],
)
def delete_warehouse(warehouse_id: int, db: Session = Depends(get_db)) -> Response:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.warehouse_id == warehouse_id)))
    affected_product_ids = sorted({row.product_id for row in stock_rows})
    db.execute(delete(WarehouseProduct).where(WarehouseProduct.warehouse_id == warehouse_id))
    db.delete(warehouse)
    db.commit()

    for product_id in affected_product_ids:
        product = recalculate_product_stocks(db, product_id)
        if product:
            publish_product_stock_event("STOCK_REPLENISHED", product.id, product.stocks)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/products",
    response_model=ProductRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create product",
    description="Creates a product for a supplier. Stock is managed separately through warehouses.",
    tags=["Products API"],
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
    summary="List products",
    description="Returns all supplier products in a QA-friendly list wrapper.",
    tags=["Products API"],
)
def list_products(db: Session = Depends(get_db)) -> ProductListRead:
    items = [serialize_product(product) for product in db.scalars(select(Product).order_by(Product.id))]
    return ProductListRead(items=items, count=len(items))


@app.get(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Get product",
    description="Returns a product by ID with aggregated stock information.",
    tags=["Products API"],
)
def get_product(product_id: int, db: Session = Depends(get_db)) -> ProductRead:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")
    return serialize_product(product)


@app.put(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Update product",
    description="Updates product fields without changing warehouse stock rows.",
    tags=["Products API"],
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
    summary="Update stock",
    description="Updates factual stock values for products in a warehouse and publishes aggregated stock events.",
    tags=["Stock API"],
)
def restock_products(warehouse_id: int, restock_in: RestockRequest, db: Session = Depends(get_db)) -> ProductListRead:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="warehouse_not_found")

    updated_products: list[ProductRead] = []
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
        db.commit()

        recalculated = recalculate_product_stocks(db, item.product_id)
        if recalculated and recalculated.id not in seen_product_ids:
            publish_product_stock_event("STOCK_REPLENISHED", recalculated.id, recalculated.stocks)
            updated_products.append(serialize_product(recalculated))
            seen_product_ids.add(recalculated.id)

    return ProductListRead(items=updated_products, count=len(updated_products))


@app.delete(
    "/products/{product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Delete product",
    description="Deletes a product by ID.",
    tags=["Products API"],
)
def delete_product(product_id: int, db: Session = Depends(get_db)) -> Response:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.product_id == product_id)))
    for row in stock_rows:
        db.delete(row)

    payload = serialize_product(product).model_dump(mode="json")
    db.delete(product)
    db.commit()
    publish_product_event("PRODUCT_DELETED", payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
