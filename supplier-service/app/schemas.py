import re
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

PHONE_PATTERN = re.compile(r"^(\+7|8)\d{10}$")
NON_DIGIT_PATTERN = re.compile(r"\D")


class SupplierBase(BaseModel):
    full_name: str = Field(min_length=3, max_length=255, examples=["Ivan Petrov"], description="Supplier full name")
    phone_number: str = Field(max_length=30, examples=["+79991234567"], description="Supplier phone number")
    email: EmailStr = Field(examples=["ivan@example.com"], description="Supplier email address")
    birth_date: date = Field(examples=["1995-05-20"], description="Supplier birth date")
    city: str = Field(min_length=2, max_length=120, examples=["Moscow"], description="Supplier city")

    @field_validator("phone_number")
    @classmethod
    def normalize_phone_number(cls, value: str) -> str:
        normalized = NON_DIGIT_PATTERN.sub("", value)
        if normalized.startswith("7") and len(normalized) == 11:
            candidate = f"+{normalized}"
        elif normalized.startswith("8") and len(normalized) == 11:
            candidate = f"+7{normalized[1:]}"
        else:
            raise ValueError("phone number must be in format +7XXXXXXXXXX or 8XXXXXXXXXX")

        if not PHONE_PATTERN.fullmatch(candidate):
            raise ValueError("phone number must be in format +7XXXXXXXXXX or 8XXXXXXXXXX")
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
            raise ValueError("phone number must be in format +7XXXXXXXXXX or 8XXXXXXXXXX")

        if not PHONE_PATTERN.fullmatch(candidate):
            raise ValueError("phone number must be in format +7XXXXXXXXXX or 8XXXXXXXXXX")
        return candidate


class SupplierRead(SupplierBase):
    id: int
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ProductBase(BaseModel):
    supplier_id: int = Field(examples=[1], gt=0, description="Product owner supplier ID")
    name: str = Field(min_length=1, max_length=255, examples=["Gaming laptop"], description="Non-empty product name")
    description: str | None = Field(default=None, max_length=1000, examples=["15-inch gaming laptop"], description="Product description")
    price: float = Field(gt=0, examples=[999.99], description="Product price, must be greater than zero")
    is_active: bool = Field(default=True, examples=[True], description="Whether the product can be purchased")
    is_archived: bool = Field(default=False, examples=[False], description="Whether the product is archived")

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("product name must not be empty")
        return normalized


class ProductCreate(ProductBase):
    pass


class ProductUpdate(BaseModel):
    supplier_id: int | None = Field(default=None, gt=0)
    name: str | None = Field(default=None, min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=1000)
    price: float | None = Field(default=None, gt=0)
    is_active: bool | None = None
    is_archived: bool | None = None

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError("product name must not be empty")
        return normalized


class ProductRead(ProductBase):
    id: int
    stocks: int = Field(ge=0, examples=[10], description="Aggregated stock across all warehouses")
    total_price: float = Field(ge=0, examples=[9999.9], description="price * stocks")
    created_at: datetime


class SupplierListRead(BaseModel):
    items: list[SupplierRead]
    count: int


class ProductListRead(BaseModel):
    items: list[ProductRead]
    count: int


class WarehouseBase(BaseModel):
    name: str = Field(min_length=2, max_length=255, examples=["North warehouse"], description="Warehouse display name")
    weekday_hours: str = Field(min_length=2, max_length=255, examples=["Mon-Fri 09:00-18:00"], description="Warehouse weekday schedule")
    address: str = Field(min_length=5, max_length=500, examples=["Moscow, Lenina street, 10"], description="Warehouse address")


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


class WarehouseListRead(BaseModel):
    items: list[WarehouseRead]
    count: int


class WarehouseStockItem(BaseModel):
    product_id: int = Field(gt=0, examples=[1])
    stocks: int = Field(ge=0, examples=[50])


class RestockRequest(BaseModel):
    items: list[WarehouseStockItem] = Field(min_length=1)
