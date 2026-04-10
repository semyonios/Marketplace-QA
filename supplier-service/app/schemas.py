import re
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

PHONE_PATTERN = re.compile(r"^(\+7|8)\d{10}$")
NON_DIGIT_PATTERN = re.compile(r"\D")


class SupplierBase(BaseModel):
    full_name: str = Field(min_length=3, max_length=255, examples=["Иван Петров"])
    phone_number: str = Field(max_length=30, examples=["+79991234567"])
    email: EmailStr = Field(examples=["ivan@example.com"])
    birth_date: date = Field(examples=["1995-05-20"])
    city: str = Field(min_length=2, max_length=120, examples=["Moscow"])

    @field_validator("phone_number")
    @classmethod
    def normalize_phone_number(cls, value: str) -> str:
        normalized = NON_DIGIT_PATTERN.sub("", value)
        if normalized.startswith("7") and len(normalized) == 11:
            candidate = f"+{normalized}"
        elif normalized.startswith("8") and len(normalized) == 11:
            candidate = f"+7{normalized[1:]}"
        else:
            raise ValueError("Номер телефона должен быть в формате +7XXXXXXXXXX или 8XXXXXXXXXX")

        if not PHONE_PATTERN.fullmatch(candidate):
            raise ValueError("Номер телефона должен быть в формате +7XXXXXXXXXX или 8XXXXXXXXXX")
        return candidate


class SupplierCreate(SupplierBase):
    pass


class SupplierUpdate(BaseModel):
    full_name: str | None = Field(default=None, min_length=3, max_length=255)
    phone_number: str | None = Field(default=None, max_length=30)
    email: EmailStr | None = None
    birth_date: date | None = None
    city: str | None = Field(default=None, min_length=2, max_length=120)

    @field_validator("phone_number")
    @classmethod
    def normalize_phone_number(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = NON_DIGIT_PATTERN.sub("", value)
        if normalized.startswith("7") and len(normalized) == 11:
            candidate = f"+{normalized}"
        elif normalized.startswith("8") and len(normalized) == 11:
            candidate = f"+7{normalized[1:]}"
        else:
            raise ValueError("Номер телефона должен быть в формате +7XXXXXXXXXX или 8XXXXXXXXXX")

        if not PHONE_PATTERN.fullmatch(candidate):
            raise ValueError("Номер телефона должен быть в формате +7XXXXXXXXXX или 8XXXXXXXXXX")
        return candidate


class SupplierRead(SupplierBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ProductBase(BaseModel):
    supplier_id: int = Field(examples=[1], gt=0)
    name: str = Field(min_length=2, max_length=255, examples=["Ноутбук"])
    description: str | None = Field(default=None, max_length=1000, examples=["Игровой ноутбук"])
    price: float = Field(ge=0, examples=[999.99])


class ProductCreate(ProductBase):
    pass


class ProductUpdate(BaseModel):
    supplier_id: int | None = Field(default=None, gt=0)
    name: str | None = Field(default=None, min_length=2, max_length=255)
    description: str | None = Field(default=None, max_length=1000)
    price: float | None = Field(default=None, ge=0)


class ProductRead(ProductBase):
    id: int
    stocks: int = Field(ge=0, examples=[10])
    total_price: float = Field(ge=0, examples=[9999.9])
    created_at: datetime


class WarehouseBase(BaseModel):
    name: str = Field(min_length=2, max_length=255, examples=["Склад Север"])
    weekday_hours: str = Field(min_length=2, max_length=255, examples=["Пн-Пт 09:00-18:00"])
    address: str = Field(min_length=5, max_length=500, examples=["Москва, ул. Ленина, 10"])


class WarehouseCreate(WarehouseBase):
    pass


class WarehouseUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=2, max_length=255)
    weekday_hours: str | None = Field(default=None, min_length=2, max_length=255)
    address: str | None = Field(default=None, min_length=5, max_length=500)


class WarehouseRead(WarehouseBase):
    id: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class WarehouseStockItem(BaseModel):
    product_id: int = Field(gt=0, examples=[1])
    stocks: int = Field(ge=0, examples=[50])


class RestockRequest(BaseModel):
    items: list[WarehouseStockItem] = Field(min_length=1)
