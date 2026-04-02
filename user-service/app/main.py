from collections.abc import Generator

from fastapi import Depends, FastAPI, HTTPException, Response, status
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine
from .kafka_producer import publish_product_event, publish_supplier_stock_event, publish_user_event
from .models import Product, User, Warehouse, WarehouseProduct
from .schemas import (
    ProductCreate,
    ProductRead,
    ProductUpdate,
    RestockRequest,
    UserCreate,
    UserRead,
    UserUpdate,
    WarehouseCreate,
    WarehouseRead,
    WarehouseUpdate,
)
from .stock_consumer import start_stock_consumer

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Supplier Service",
    description="CRUD для поставщиков, товаров и складов с отправкой событий в Kafka.",
    version="1.0.0",
    openapi_tags=[
        {"name": "Служебное API", "description": "Технические и health-ручки сервиса поставщика"},
        {"name": "API для управления поставщиками", "description": "Создание, получение, изменение и удаление поставщиков"},
        {"name": "API для управления товарами", "description": "Создание и редактирование карточек товаров"},
        {"name": "API для управления остатками", "description": "Ручки для работы с остатками товаров на складах"},
        {"name": "API для управления складами", "description": "Создание, изменение, удаление и просмотр складов"},
    ],
)


@app.on_event("startup")
def startup() -> None:
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


@app.get(
    "/health",
    summary="Проверка supplier-service",
    description="Возвращает простой статус доступности сервиса поставщика",
    tags=["Служебное API"],
)
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/users",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать поставщика",
    description="Создаёт нового поставщика в базе supplier-service",
    tags=["API для управления поставщиками"],
)
def create_user(user_in: UserCreate, db: Session = Depends(get_db)) -> User:
    user = User(**user_in.model_dump())
    db.add(user)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Поставщик с таким email или телефоном уже существует") from exc

    db.refresh(user)
    publish_user_event("USER_CREATED", UserRead.model_validate(user).model_dump(mode="json"))
    return user


@app.get(
    "/users",
    response_model=list[UserRead],
    summary="Список поставщиков",
    description="Возвращает список всех поставщиков",
    tags=["API для управления поставщиками"],
)
def list_users(db: Session = Depends(get_db)) -> list[User]:
    return list(db.scalars(select(User).order_by(User.id)))


@app.get(
    "/users/{user_id}",
    response_model=UserRead,
    summary="Получить поставщика",
    description="Возвращает поставщика по его идентификатору",
    tags=["API для управления поставщиками"],
)
def get_user(user_id: int, db: Session = Depends(get_db)) -> User:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Поставщик не найден")
    return user


@app.put(
    "/users/{user_id}",
    response_model=UserRead,
    summary="Обновить поставщика",
    description="Обновляет данные поставщика по идентификатору",
    tags=["API для управления поставщиками"],
)
def update_user(user_id: int, user_in: UserUpdate, db: Session = Depends(get_db)) -> User:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Поставщик не найден")

    for field, value in user_in.model_dump(exclude_unset=True).items():
        setattr(user, field, value)

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Поставщик с таким email или телефоном уже существует") from exc

    db.refresh(user)
    publish_user_event("USER_UPDATED", UserRead.model_validate(user).model_dump(mode="json"))
    return user


@app.delete(
    "/users/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить поставщика",
    description="Удаляет поставщика по идентификатору",
    tags=["API для управления поставщиками"],
)
def delete_user(user_id: int, db: Session = Depends(get_db)) -> Response:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Поставщик не найден")

    payload = UserRead.model_validate(user).model_dump(mode="json")
    db.delete(user)
    db.commit()
    publish_user_event("USER_DELETED", payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/warehouses",
    response_model=WarehouseRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать склад",
    description="Создаёт новый склад с названием, графиком работы по будням и адресом",
    tags=["API для управления складами"],
)
def create_warehouse(warehouse_in: WarehouseCreate, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = Warehouse(**warehouse_in.model_dump())
    db.add(warehouse)
    db.commit()
    db.refresh(warehouse)
    return warehouse


@app.get(
    "/warehouses",
    response_model=list[WarehouseRead],
    summary="Список складов",
    description="Возвращает список всех складов поставщика",
    tags=["API для управления складами"],
)
def list_warehouses(db: Session = Depends(get_db)) -> list[Warehouse]:
    return list(db.scalars(select(Warehouse).order_by(Warehouse.id)))


@app.get(
    "/warehouses/{warehouse_id}",
    response_model=WarehouseRead,
    summary="Получить склад",
    description="Возвращает склад по его идентификатору",
    tags=["API для управления складами"],
)
def get_warehouse(warehouse_id: int, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="Склад не найден")
    return warehouse


@app.put(
    "/warehouses/{warehouse_id}",
    response_model=WarehouseRead,
    summary="Обновить склад",
    description="Обновляет название, график работы по будням и адрес склада",
    tags=["API для управления складами"],
)
def update_warehouse(warehouse_id: int, warehouse_in: WarehouseUpdate, db: Session = Depends(get_db)) -> Warehouse:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="Склад не найден")

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
    description="Удаляет склад и обнуляет остатки товаров, которые были размещены на этом складе",
    tags=["API для управления складами"],
)
def delete_warehouse(warehouse_id: int, db: Session = Depends(get_db)) -> Response:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="Склад не найден")

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.warehouse_id == warehouse_id)))
    affected_product_ids = sorted({row.product_id for row in stock_rows})
    db.execute(delete(WarehouseProduct).where(WarehouseProduct.warehouse_id == warehouse_id))
    db.delete(warehouse)
    db.commit()

    for product_id in affected_product_ids:
        product = recalculate_product_stocks(db, product_id)
        if product:
            publish_supplier_stock_event("STOCK_RESTOCKED", product.id, product.stocks)

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/products",
    response_model=ProductRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать товар",
    description="Создаёт новый товар и привязывает его к поставщику. Остатки управляются отдельно через склады.",
    tags=["API для управления товарами"],
)
def create_product(product_in: ProductCreate, db: Session = Depends(get_db)) -> ProductRead:
    supplier = db.get(User, product_in.supplier_id)
    if not supplier:
        raise HTTPException(status_code=404, detail="Поставщик не найден")

    product = Product(**product_in.model_dump(), stocks=0)
    db.add(product)
    db.commit()
    db.refresh(product)

    payload = serialize_product(product).model_dump(mode="json")
    publish_product_event("PRODUCT_CREATED", payload)
    return serialize_product(product)


@app.get(
    "/products",
    response_model=list[ProductRead],
    summary="Список товаров",
    description="Возвращает список всех товаров поставщика вместе с общей стоимостью остатков",
    tags=["API для управления товарами"],
)
def list_products(db: Session = Depends(get_db)) -> list[ProductRead]:
    return [serialize_product(product) for product in db.scalars(select(Product).order_by(Product.id))]


@app.get(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Получить товар",
    description="Возвращает товар по его идентификатору вместе с общей стоимостью остатков",
    tags=["API для управления товарами"],
)
def get_product(product_id: int, db: Session = Depends(get_db)) -> ProductRead:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Товар не найден")
    return serialize_product(product)


@app.put(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Обновить товар",
    description="Обновляет данные товара без изменения складских остатков",
    tags=["API для управления товарами"],
)
def update_product(product_id: int, product_in: ProductUpdate, db: Session = Depends(get_db)) -> ProductRead:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Товар не найден")

    updates = product_in.model_dump(exclude_unset=True)
    supplier_id = updates.get("supplier_id")
    if supplier_id is not None and not db.get(User, supplier_id):
        raise HTTPException(status_code=404, detail="Поставщик не найден")

    for field, value in updates.items():
        setattr(product, field, value)

    db.commit()
    db.refresh(product)

    payload = serialize_product(product).model_dump(mode="json")
    publish_product_event("PRODUCT_UPDATED", payload)
    return serialize_product(product)


@app.post(
    "/warehouses/{warehouse_id}/stocks",
    response_model=list[ProductRead],
    summary="Управление остатками",
    description="Принимает идентификатор склада в URL и набор товаров с фактическими остатками в теле запроса, обновляет их и отправляет новые агрегированные остатки в Kafka",
    tags=["API для управления остатками"],
)
def restock_products(warehouse_id: int, restock_in: RestockRequest, db: Session = Depends(get_db)) -> list[ProductRead]:
    warehouse = db.get(Warehouse, warehouse_id)
    if not warehouse:
        raise HTTPException(status_code=404, detail="Склад не найден")

    updated_products: list[ProductRead] = []
    seen_product_ids: set[int] = set()

    for item in restock_in.items:
        product = db.get(Product, item.product_id)
        if not product:
            raise HTTPException(status_code=404, detail=f"Товар {item.product_id} не найден")

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
            publish_supplier_stock_event("STOCK_RESTOCKED", recalculated.id, recalculated.stocks)
            updated_products.append(serialize_product(recalculated))
            seen_product_ids.add(recalculated.id)

    return updated_products


@app.delete(
    "/products/{product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить товар",
    description="Удаляет товар по идентификатору",
    tags=["API для управления товарами"],
)
def delete_product(product_id: int, db: Session = Depends(get_db)) -> Response:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Товар не найден")

    stock_rows = list(db.scalars(select(WarehouseProduct).where(WarehouseProduct.product_id == product_id)))
    for row in stock_rows:
        db.delete(row)

    payload = serialize_product(product).model_dump(mode="json")
    db.delete(product)
    db.commit()
    publish_product_event("PRODUCT_DELETED", payload)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
