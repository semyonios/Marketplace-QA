import os

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://order_app:order_app@localhost:5434/order_db",
)
