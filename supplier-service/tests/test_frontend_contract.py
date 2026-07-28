from datetime import datetime, timezone

from app.main import serialize_product
from app.models import Product


def test_product_representation_exposes_total_reserved_and_available_stock() -> None:
    product = Product(
        id=1001,
        supplier_id=201,
        name="QA Product",
        description=None,
        price=100.0,
        stocks=10,
        reserved_stocks=3,
        is_active=True,
        is_archived=False,
        created_at=datetime.now(timezone.utc),
    )

    representation = serialize_product(product)

    assert representation.stocks == 10
    assert representation.reserved_stocks == 3
    assert representation.available_stocks == 7
