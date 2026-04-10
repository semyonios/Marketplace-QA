import logging
from collections import defaultdict
from collections.abc import Generator

from fastapi import Depends, FastAPI, HTTPException, Response, status
from sqlalchemy import inspect, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .database import Base, SessionLocal, engine
from .error_handlers import register_exception_handlers
from .kafka_consumers import start_consumers, sync_products_from_supplier
from .kafka_producer import publish_order_created
from .models import CartItem, Favorite, Order, OrderItem, Product, User
from .schemas import (
    CartCreate,
    CartItemRead,
    CartQuantityUpdate,
    CartRead,
    FavoriteCreate,
    FavoriteListRead,
    FavoriteRead,
    OrderCreate,
    OrderCreateItem,
    OrderItemRead,
    OrderListRead,
    OrderRead,
    ProductListRead,
    ProductRead,
    ProductSummary,
    UserCreate,
    UserListRead,
    UserRead,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Сервис покупателя",
    description="API покупателя с локальной копией каталога, избранным, корзиной и заказами. Данные по товарам синхронизируются через Kafka.",
    version="3.0.0",
    openapi_tags=[
        {"name": "Служебное API", "description": "Технические ручки сервиса и проверка доступности."},
        {"name": "API покупателей", "description": "Создание и просмотр покупателей."},
        {"name": "API каталога", "description": "Просмотр локальной копии каталога товаров."},
        {"name": "API избранного", "description": "Работа с избранными товарами."},
        {"name": "API корзины", "description": "Работа с корзиной и количеством товаров."},
        {"name": "API заказов", "description": "Создание, просмотр и отмена заказов."},
    ],
)

register_exception_handlers(app)


def ensure_customer_schema() -> None:
    Base.metadata.create_all(bind=engine)

    inspector = inspect(engine)
    order_columns = {column["name"] for column in inspector.get_columns("orders")} if "orders" in inspector.get_table_names() else set()
    legacy_columns = {"product_id", "quantity"}

    if order_columns and legacy_columns.issubset(order_columns):
        with engine.begin() as connection:
            if "order_number" not in order_columns:
                connection.execute(text("ALTER TABLE orders ADD COLUMN order_number VARCHAR(50)"))
            if "total_price" not in order_columns:
                connection.execute(text("ALTER TABLE orders ADD COLUMN total_price DOUBLE PRECISION NOT NULL DEFAULT 0"))
            connection.execute(text("ALTER TABLE orders ALTER COLUMN product_id DROP NOT NULL"))
            connection.execute(text("ALTER TABLE orders ALTER COLUMN quantity DROP NOT NULL"))

    product_columns = {column["name"] for column in inspector.get_columns("products")} if "products" in inspector.get_table_names() else set()
    with engine.begin() as connection:
        if "is_active" not in product_columns:
            connection.execute(text("ALTER TABLE products ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE"))
        if "is_archived" not in product_columns:
            connection.execute(text("ALTER TABLE products ADD COLUMN is_archived BOOLEAN NOT NULL DEFAULT FALSE"))


@app.on_event("startup")
def startup() -> None:
    ensure_customer_schema()
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
        is_active=product.is_active,
        is_archived=product.is_archived,
        total_price=round(product.price * product.stocks, 2),
        created_at=product.created_at,
    )


def serialize_product_summary(product: Product) -> ProductSummary:
    return ProductSummary(
        id=product.id,
        name=product.name,
        price=product.price,
        stocks=product.stocks,
        is_active=product.is_active,
        is_archived=product.is_archived,
    )


def serialize_favorite(db: Session, favorite: Favorite) -> FavoriteRead:
    product = get_existing_product(db, favorite.product_id)
    return FavoriteRead(
        id=favorite.id,
        user_id=favorite.user_id,
        product=serialize_product_summary(product),
        created_at=favorite.created_at,
    )


def serialize_cart_item(db: Session, cart_item: CartItem) -> CartItemRead:
    product = get_existing_product(db, cart_item.product_id)
    return CartItemRead(
        id=cart_item.id,
        user_id=cart_item.user_id,
        product=serialize_product_summary(product),
        quantity=cart_item.quantity,
        unit_price=product.price,
        total_price=round(product.price * cart_item.quantity, 2),
        created_at=cart_item.created_at,
        updated_at=cart_item.updated_at,
    )


def serialize_cart(db: Session, cart_items: list[CartItem]) -> CartRead:
    items = [serialize_cart_item(db, cart_item) for cart_item in cart_items]
    return CartRead(
        items=items,
        count=len(items),
        total_items_count=sum(item.quantity for item in items),
        total_price=round(sum(item.total_price for item in items), 2),
    )


def serialize_order_item(db: Session, order_item: OrderItem) -> OrderItemRead:
    product = get_existing_product(db, order_item.product_id)
    return OrderItemRead(
        id=order_item.id,
        product=serialize_product_summary(product),
        quantity=order_item.quantity,
        unit_price=order_item.unit_price,
        total_price=order_item.total_price,
        created_at=order_item.created_at,
    )


def serialize_order(db: Session, order: Order) -> OrderRead:
    order_items = list(db.scalars(select(OrderItem).where(OrderItem.order_id == order.id).order_by(OrderItem.id)))
    return OrderRead(
        id=order.id,
        user_id=order.user_id,
        order_number=order.order_number or generate_order_number(order.id),
        status=order.status,
        items=[serialize_order_item(db, order_item) for order_item in order_items],
        total_items_count=sum(order_item.quantity for order_item in order_items),
        total_price=round(order.total_price, 2),
        created_at=order.created_at,
    )


def generate_order_number(order_id: int) -> str:
    return f"ORD-{order_id:06d}"


@app.get(
    "/health",
    summary="Проверка доступности",
    description="Возвращает статус доступности `customer-service`.",
    tags=["Служебное API"],
)
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/users",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать покупателя",
    description="Создаёт нового покупателя.",
    tags=["API покупателей"],
)
def create_user(user_in: UserCreate, db: Session = Depends(get_db)) -> User:
    user = User(**user_in.model_dump())
    db.add(user)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="user_conflict") from exc
    db.refresh(user)
    return user


@app.get(
    "/users",
    response_model=UserListRead,
    summary="Получить список покупателей",
    description="Возвращает список всех покупателей в формате `items + count`.",
    tags=["API покупателей"],
)
def list_users(db: Session = Depends(get_db)) -> UserListRead:
    items = list(db.scalars(select(User).order_by(User.id)))
    return UserListRead(items=items, count=len(items))


@app.get(
    "/users/{user_id}",
    response_model=UserRead,
    summary="Получить покупателя",
    description="Возвращает покупателя по идентификатору.",
    tags=["API покупателей"],
)
def get_user(user_id: int, db: Session = Depends(get_db)) -> User:
    return validate_user(db, user_id)


@app.get(
    "/products",
    response_model=ProductListRead,
    summary="Получить каталог товаров",
    description="Возвращает локальную копию каталога товаров в формате `items + count`.",
    tags=["API каталога"],
)
def list_products(db: Session = Depends(get_db)) -> ProductListRead:
    items = [serialize_product(product) for product in db.scalars(select(Product).order_by(Product.id))]
    return ProductListRead(items=items, count=len(items))


@app.get(
    "/products/{product_id}",
    response_model=ProductRead,
    summary="Получить товар",
    description="Возвращает товар из локальной копии каталога по идентификатору.",
    tags=["API каталога"],
)
def get_product(product_id: int, db: Session = Depends(get_db)) -> ProductRead:
    product = get_existing_product(db, product_id)
    return serialize_product(product)


@app.post(
    "/favorites",
    response_model=FavoriteRead,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить в избранное",
    description="Добавляет товар в избранное покупателя. Повторный запрос возвращает уже существующую запись.",
    tags=["API избранного"],
)
def add_to_favorites(favorite_in: FavoriteCreate, db: Session = Depends(get_db)) -> FavoriteRead:
    validate_user_and_product(db, favorite_in.user_id, favorite_in.product_id)

    favorite = db.scalar(
        select(Favorite).where(Favorite.user_id == favorite_in.user_id, Favorite.product_id == favorite_in.product_id)
    )
    if not favorite:
        favorite = Favorite(**favorite_in.model_dump())
        db.add(favorite)
        db.commit()
        db.refresh(favorite)
    return serialize_favorite(db, favorite)


@app.get(
    "/favorites",
    response_model=FavoriteListRead,
    summary="Получить избранное",
    description="Возвращает все товары из избранного покупателя в формате `items + count`.",
    tags=["API избранного"],
)
def list_favorites(user_id: int, db: Session = Depends(get_db)) -> FavoriteListRead:
    validate_user(db, user_id)
    favorites = list(db.scalars(select(Favorite).where(Favorite.user_id == user_id).order_by(Favorite.id)))
    return FavoriteListRead(items=[serialize_favorite(db, favorite) for favorite in favorites], count=len(favorites))


@app.delete(
    "/favorites/{product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить из избранного",
    description="Удаляет товар из избранного по `user_id` и `product_id`.",
    tags=["API избранного"],
)
def delete_favorite(product_id: int, user_id: int, db: Session = Depends(get_db)) -> Response:
    validate_user(db, user_id)
    favorite = db.scalar(select(Favorite).where(Favorite.user_id == user_id, Favorite.product_id == product_id))
    if not favorite:
        raise HTTPException(status_code=404, detail="favorite_not_found")
    db.delete(favorite)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get(
    "/cart",
    response_model=CartRead,
    summary="Получить корзину",
    description="Возвращает корзину покупателя с количеством позиций, общим количеством товаров и итоговой стоимостью.",
    tags=["API корзины"],
)
def get_cart(user_id: int, db: Session = Depends(get_db)) -> CartRead:
    validate_user(db, user_id)
    cart_items = list(db.scalars(select(CartItem).where(CartItem.user_id == user_id).order_by(CartItem.id)))
    return serialize_cart(db, cart_items)


@app.post(
    "/cart",
    response_model=CartItemRead,
    status_code=status.HTTP_201_CREATED,
    summary="Добавить в корзину",
    description="Добавляет товар в корзину. Если товар уже есть в корзине, количество увеличивается.",
    tags=["API корзины"],
)
def add_to_cart(cart_in: CartCreate, db: Session = Depends(get_db)) -> CartItemRead:
    product = validate_user_and_product(db, cart_in.user_id, cart_in.product_id)
    ensure_product_can_be_purchased(product)
    cart_item = db.scalar(
        select(CartItem).where(CartItem.user_id == cart_in.user_id, CartItem.product_id == cart_in.product_id)
    )

    total_quantity = cart_in.quantity + (cart_item.quantity if cart_item else 0)
    ensure_requested_quantity_available(total_quantity, product.stocks, cart_in.product_id)

    if cart_item:
        cart_item.quantity = total_quantity
    else:
        cart_item = CartItem(**cart_in.model_dump())
        db.add(cart_item)

    db.commit()
    db.refresh(cart_item)
    return serialize_cart_item(db, cart_item)


@app.patch(
    "/cart/{product_id}",
    response_model=CartItemRead,
    summary="Изменить количество в корзине",
    description="Изменяет количество товара, который уже находится в корзине покупателя.",
    tags=["API корзины"],
)
def update_cart_item(product_id: int, cart_update: CartQuantityUpdate, db: Session = Depends(get_db)) -> CartItemRead:
    product = validate_user_and_product(db, cart_update.user_id, product_id)
    ensure_product_can_be_purchased(product)
    cart_item = db.scalar(select(CartItem).where(CartItem.user_id == cart_update.user_id, CartItem.product_id == product_id))
    if not cart_item:
        raise HTTPException(status_code=404, detail="cart_item_not_found")

    ensure_requested_quantity_available(cart_update.quantity, product.stocks, product_id)
    cart_item.quantity = cart_update.quantity
    db.commit()
    db.refresh(cart_item)
    return serialize_cart_item(db, cart_item)


@app.delete(
    "/cart/{product_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Удалить из корзины",
    description="Удаляет товар из корзины по `user_id` и `product_id`.",
    tags=["API корзины"],
)
def delete_cart_item(product_id: int, user_id: int, db: Session = Depends(get_db)) -> Response:
    validate_user(db, user_id)
    cart_item = db.scalar(select(CartItem).where(CartItem.user_id == user_id, CartItem.product_id == product_id))
    if not cart_item:
        raise HTTPException(status_code=404, detail="cart_item_not_found")
    db.delete(cart_item)
    db.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/orders",
    response_model=OrderRead,
    status_code=status.HTTP_201_CREATED,
    summary="Создать заказ",
    description="Создаёт заказ из переданных `items` или из текущей корзины. После успешного создания корзина очищается.",
    tags=["API заказов"],
)
def create_order(order_in: OrderCreate, db: Session = Depends(get_db)) -> OrderRead:
    validate_user(db, order_in.user_id)

    order_request_items = build_order_items_from_request(db, order_in)
    ensure_order_items_available(db, order_request_items)

    order = Order(user_id=order_in.user_id, status="created", total_price=0)
    db.add(order)
    db.flush()

    order_total = 0.0
    order_events: list[tuple[int, int]] = []

    for request_item in order_request_items:
        product = get_existing_product(db, request_item.product_id)
        total_price = round(product.price * request_item.quantity, 2)
        order_item = OrderItem(
            order_id=order.id,
            product_id=request_item.product_id,
            quantity=request_item.quantity,
            unit_price=product.price,
            total_price=total_price,
        )
        db.add(order_item)
        order_total += total_price
        order_events.append((request_item.product_id, request_item.quantity))

    order.total_price = round(order_total, 2)
    order.order_number = generate_order_number(order.id)

    cart_items = list(db.scalars(select(CartItem).where(CartItem.user_id == order_in.user_id)))
    for cart_item in cart_items:
        db.delete(cart_item)

    db.commit()
    db.refresh(order)
    logger.info("order created order_id=%s user_id=%s total_items_count=%s total_price=%s", order.id, order.user_id, sum(quantity for _, quantity in order_events), order.total_price)

    for product_id, quantity in order_events:
        publish_order_created(product_id, quantity)

    return serialize_order(db, order)


@app.post(
    "/orders/{order_id}/cancel",
    response_model=OrderRead,
    summary="Отменить заказ",
    description="Переводит заказ в статус `cancelled`, если он ещё не был отменён.",
    tags=["API заказов"],
)
def cancel_order(order_id: int, user_id: int, db: Session = Depends(get_db)) -> OrderRead:
    validate_user(db, user_id)
    order = db.scalar(select(Order).where(Order.id == order_id, Order.user_id == user_id, Order.order_number.is_not(None)))
    if not order:
        raise HTTPException(status_code=404, detail="order_not_found")
    if order.status == "cancelled":
        raise HTTPException(status_code=409, detail="order_already_cancelled")

    order.status = "cancelled"
    db.commit()
    db.refresh(order)
    logger.info("order cancelled order_id=%s user_id=%s", order.id, order.user_id)
    return serialize_order(db, order)


@app.get(
    "/orders",
    response_model=OrderListRead,
    summary="Получить список заказов",
    description="Возвращает все заказы покупателя в формате `items + count`.",
    tags=["API заказов"],
)
def list_orders(user_id: int, db: Session = Depends(get_db)) -> OrderListRead:
    validate_user(db, user_id)
    orders = list(
        db.scalars(
            select(Order)
            .where(Order.user_id == user_id, Order.order_number.is_not(None))
            .order_by(Order.id.desc())
        )
    )
    return OrderListRead(items=[serialize_order(db, order) for order in orders], count=len(orders))


@app.get(
    "/orders/{order_id}",
    response_model=OrderRead,
    summary="Получить заказ",
    description="Возвращает заказ по `order_id` и `user_id` вместе с позициями заказа.",
    tags=["API заказов"],
)
def get_order(order_id: int, user_id: int, db: Session = Depends(get_db)) -> OrderRead:
    validate_user(db, user_id)
    order = db.scalar(select(Order).where(Order.id == order_id, Order.user_id == user_id, Order.order_number.is_not(None)))
    if not order:
        raise HTTPException(status_code=404, detail="order_not_found")
    return serialize_order(db, order)


def build_order_items_from_request(db: Session, order_in: OrderCreate) -> list[OrderCreateItem]:
    if order_in.items:
        return aggregate_order_items(order_in.items)

    cart_items = list(db.scalars(select(CartItem).where(CartItem.user_id == order_in.user_id).order_by(CartItem.id)))
    if not cart_items:
        raise HTTPException(status_code=400, detail="cart_is_empty")

    return [OrderCreateItem(product_id=cart_item.product_id, quantity=cart_item.quantity) for cart_item in cart_items]


def aggregate_order_items(items: list[OrderCreateItem]) -> list[OrderCreateItem]:
    grouped_quantities: dict[int, int] = defaultdict(int)
    for item in items:
        grouped_quantities[item.product_id] += item.quantity
    return [OrderCreateItem(product_id=product_id, quantity=quantity) for product_id, quantity in grouped_quantities.items()]


def ensure_order_items_available(db: Session, items: list[OrderCreateItem]) -> None:
    for item in items:
        product = get_existing_product(db, item.product_id)
        ensure_product_can_be_purchased(product)
        ensure_requested_quantity_available(item.quantity, product.stocks, item.product_id)


def ensure_requested_quantity_available(requested_quantity: int, available_stocks: int, product_id: int) -> None:
    if requested_quantity > available_stocks:
        logger.warning("insufficient stock for product_id=%s requested=%s available=%s", product_id, requested_quantity, available_stocks)
        raise HTTPException(status_code=409, detail="insufficient_stock")


def ensure_product_can_be_purchased(product: Product) -> None:
    if product.is_archived:
        raise HTTPException(status_code=409, detail="product_archived")
    if not product.is_active:
        raise HTTPException(status_code=409, detail="product_inactive")


def validate_user(db: Session, user_id: int) -> User:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    return user


def get_existing_product(db: Session, product_id: int) -> Product:
    product = db.get(Product, product_id)
    if not product:
        raise HTTPException(status_code=404, detail="product_not_found")
    return product


def validate_user_and_product(db: Session, user_id: int, product_id: int) -> Product:
    validate_user(db, user_id)
    return get_existing_product(db, product_id)
