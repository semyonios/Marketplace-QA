import logging
from collections import defaultdict
from collections.abc import Generator

from fastapi import Depends, FastAPI, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine
from .error_handlers import register_exception_handlers
from .kafka_consumers import start_consumers, sync_products_from_supplier
from .kafka_producer import publish_order_created
from .models import Order, Product, User
from .schemas import (
    CartRequest,
    FavoriteRequest,
    OrderListRead,
    OrderRead,
    ProductRead,
    PurchaseItem,
    PurchaseRequest,
    UserCreate,
    UserRead,
)

logging.basicConfig(level=logging.INFO)

Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Customer Service",
    description="Покупатели, корзина, избранное и покупка товаров через Kafka.",
    version="1.0.0",
    openapi_tags=[
        {"name": "Служебное API", "description": "Технические и health-ручки сервиса покупателя"},
        {"name": "API для управления покупателями", "description": "Создание и просмотр покупателей"},
        {"name": "API для просмотра товаров", "description": "Просмотр локальной копии товаров и их остатков"},
        {"name": "API для избранного", "description": "Работа с избранными товарами"},
        {"name": "API для корзины", "description": "Добавление, просмотр и удаление товаров в корзине"},
        {"name": "API для покупок", "description": "Оформление покупок и просмотр заказов"},
    ],
)

register_exception_handlers(app)


@app.on_event("startup")
def startup() -> None:
    start_consumers()
    sync_products_from_supplier()



def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()



def serialize_product(product: Product) -> ProductRead:
    return ProductRead(
        id=product.id,
        name=product.name,
        description=product.description,
        price=product.price,
        stocks=product.stocks,
        total_price=round(product.price * product.stocks, 2),
        created_at=product.created_at,
    )



def serialize_order(db: Session, order: Order) -> OrderRead:
    product = db.get(Product, order.product_id)
    unit_price = product.price if product else 0.0
    return OrderRead(
        id=order.id,
        user_id=order.user_id,
        product_id=order.product_id,
        quantity=order.quantity,
        unit_price=unit_price,
        total_price=round(unit_price * order.quantity, 2),
        status=order.status,
        created_at=order.created_at,
    )



def serialize_order_list(db: Session, orders: list[Order]) -> OrderListRead:
    items = [serialize_order(db, order) for order in orders]
    return OrderListRead(items=items, total_price=round(sum(item.total_price for item in items), 2))


@app.get(
    "/health",
    summary="Проверка customer-service",
    description="Возвращает простой статус доступности сервиса покупателя",
    tags=["Служебное API"],
)
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/users",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать покупателя",
    description="Создаёт нового покупателя в базе customer-service",
    tags=["API для управления покупателями"],
)
def create_user(user_in: UserCreate, db: Session = Depends(get_db)) -> User:
    user = User(**user_in.model_dump())
    db.add(user)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Пользователь с таким email уже существует") from exc
    db.refresh(user)
    return user


@app.get(
    "/users",
    response_model=list[UserRead],
    summary="Список покупателей",
    description="Возвращает список всех покупателей",
    tags=["API для управления покупателями"],
)
def list_users(db: Session = Depends(get_db)) -> list[User]:
    return list(db.scalars(select(User).order_by(User.id)))


@app.get(
    "/users/{user_id}",
    response_model=UserRead,
    summary="Получить покупателя",
    description="Возвращает покупателя по его идентификатору",
    tags=["API для управления покупателями"],
)
def get_user(user_id: int, db: Session = Depends(get_db)) -> User:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return user


@app.get(
    "/products",
    response_model=list[ProductRead],
    summary="Список товаров",
    description="Возвращает локальную копию товаров с актуальными остатками и их общей стоимостью, полученную через Kafka",
    tags=["API для просмотра товаров"],
)
def list_products(db: Session = Depends(get_db)) -> list[ProductRead]:
    return [serialize_product(product) for product in db.scalars(select(Product).order_by(Product.id))]


@app.get(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Получить товар",
    description="Возвращает локальную копию товара по идентификатору с актуальными остатками и общей стоимостью",
    tags=["API для просмотра товаров"],
)
def get_product(product_id: int, db: Session = Depends(get_db)) -> ProductRead:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Товар не найден")
    return serialize_product(product)


@app.post(
    "/favorites",
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить в избранное",
    description="Создаёт запись со статусом favorite для выбранного товара и возвращает его стоимость",
    tags=["API для избранного"],
)
def add_to_favorites(favorite_in: FavoriteRequest, db: Session = Depends(get_db)) -> OrderRead:
    validate_user_and_product(db, favorite_in.user_id, favorite_in.product_id)
    order = Order(user_id=favorite_in.user_id, product_id=favorite_in.product_id, quantity=1, status="favorite")
    db.add(order)
    db.commit()
    db.refresh(order)
    return serialize_order(db, order)


@app.delete(
    "/favorites",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить из избранного",
    description="Удаляет товар из избранного по user_id и product_id",
    tags=["API для избранного"],
)
def delete_favorite(user_id: int, product_id: int, db: Session = Depends(get_db)) -> Response:
    order = db.scalar(
        select(Order).where(Order.user_id == user_id, Order.product_id == product_id, Order.status == "favorite")
    )
    if not order:
        raise HTTPException(status_code=404, detail="Избранное не найдено")
    db.delete(order)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get(
    "/cart",
    response_model=OrderListRead,
    summary="Получить корзину",
    description="Возвращает товары из корзины выбранного покупателя по user_id вместе с общей стоимостью корзины",
    tags=["API для корзины"],
)
def get_cart(user_id: int, db: Session = Depends(get_db)) -> OrderListRead:
    validate_user(db, user_id)
    orders = list(db.scalars(select(Order).where(Order.user_id == user_id, Order.status == "cart").order_by(Order.id)))
    return serialize_order_list(db, orders)


@app.post(
    "/cart",
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить в корзину",
    description="Добавляет товар в корзину. Если товар уже есть в корзине, увеличивает его количество в той же записи. Нельзя превысить доступные остатки. В ответе возвращает стоимость позиции.",
    tags=["API для корзины"],
)
def add_to_cart(cart_in: CartRequest, db: Session = Depends(get_db)) -> OrderRead:
    product = validate_user_and_product(db, cart_in.user_id, cart_in.product_id)
    cart_order = db.scalar(
        select(Order).where(
            Order.user_id == cart_in.user_id,
            Order.product_id == cart_in.product_id,
            Order.status == "cart",
        )
    )

    current_quantity = cart_order.quantity if cart_order else 0
    total_quantity = current_quantity + cart_in.quantity
    ensure_requested_quantity_available(total_quantity, product.stocks, cart_in.product_id)

    if cart_order:
        cart_order.quantity = total_quantity
        db.commit()
        db.refresh(cart_order)
        return serialize_order(db, cart_order)

    order = Order(user_id=cart_in.user_id, product_id=cart_in.product_id, quantity=cart_in.quantity, status="cart")
    db.add(order)
    db.commit()
    db.refresh(order)
    return serialize_order(db, order)


@app.delete(
    "/cart",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить из корзины",
    description="Удаляет товар из корзины по user_id и product_id",
    tags=["API для корзины"],
)
def delete_cart_item(user_id: int, product_id: int, db: Session = Depends(get_db)) -> Response:
    order = db.scalar(select(Order).where(Order.user_id == user_id, Order.product_id == product_id, Order.status == "cart"))
    if not order:
        raise HTTPException(status_code=404, detail="Товар в корзине не найден")
    db.delete(order)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get(
    "/purchases",
    response_model=OrderListRead,
    summary="Список покупок",
    description="Возвращает оформленные покупки выбранного покупателя по user_id вместе с общей стоимостью",
    tags=["API для покупок"],
)
def get_purchases(user_id: int, db: Session = Depends(get_db)) -> OrderListRead:
    validate_user(db, user_id)
    orders = list(db.scalars(select(Order).where(Order.user_id == user_id, Order.status == "purchased").order_by(Order.id)))
    return serialize_order_list(db, orders)


@app.post(
    "/purchase",
    response_model=OrderListRead,
    summary="Оформить покупку",
    description="Покупает товары напрямую из тела запроса или, если список не передан, оформляет все товары из корзины. Перед покупкой проверяет доступные остатки и возвращает итоговую стоимость покупки.",
    tags=["API для покупок"],
)
def purchase_items(purchase_in: PurchaseRequest, db: Session = Depends(get_db)) -> OrderListRead:
    validate_user(db, purchase_in.user_id)

    purchase_items_data = build_purchase_items(db, purchase_in)
    ensure_purchase_items_available(db, purchase_items_data)

    purchased_orders: list[Order] = []
    order_events: list[tuple[int, int]] = []

    for item in purchase_items_data:
        purchased = Order(user_id=purchase_in.user_id, product_id=item.product_id, quantity=item.quantity, status="purchased")
        db.add(purchased)
        db.flush()
        db.refresh(purchased)
        purchased_orders.append(purchased)
        order_events.append((item.product_id, item.quantity))

    if not purchase_in.items:
        cart_rows = list(db.scalars(select(Order).where(Order.user_id == purchase_in.user_id, Order.status == "cart")))
        for row in cart_rows:
            db.delete(row)

    db.commit()
    for order in purchased_orders:
        db.refresh(order)
    for product_id, quantity in order_events:
        publish_order_created(product_id, quantity)
    return serialize_order_list(db, purchased_orders)



def build_purchase_items(db: Session, purchase_in: PurchaseRequest) -> list[PurchaseItem]:
    if purchase_in.items:
        return aggregate_purchase_items(purchase_in.items)

    cart_rows = list(db.scalars(select(Order).where(Order.user_id == purchase_in.user_id, Order.status == "cart")))
    if not cart_rows:
        raise HTTPException(status_code=400, detail="Корзина пуста и товары для покупки не переданы")

    return aggregate_purchase_items(
        [PurchaseItem(product_id=row.product_id, quantity=row.quantity) for row in cart_rows]
    )



def aggregate_purchase_items(items: list[PurchaseItem]) -> list[PurchaseItem]:
    grouped: dict[int, int] = defaultdict(int)
    for item in items:
        grouped[item.product_id] += item.quantity
    return [PurchaseItem(product_id=product_id, quantity=quantity) for product_id, quantity in grouped.items()]



def ensure_purchase_items_available(db: Session, items: list[PurchaseItem]) -> None:
    for item in items:
        product = db.get(Product, item.product_id)
        if not product:
            raise HTTPException(status_code=404, detail=f"Товар {item.product_id} не найден")
        ensure_requested_quantity_available(item.quantity, product.stocks, item.product_id)



def ensure_requested_quantity_available(requested_quantity: int, available_stocks: int, product_id: int) -> None:
    if requested_quantity > available_stocks:
        raise HTTPException(
            status_code=400,
            detail=f"Недостаточно товара {product_id} на складе. Доступно: {available_stocks}",
        )



def validate_user(db: Session, user_id: int) -> User:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    return user



def validate_user_and_product(db: Session, user_id: int, product_id: int) -> Product:
    validate_user(db, user_id)
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="Товар не найден")
    return product
