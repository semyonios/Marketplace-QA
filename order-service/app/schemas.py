from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str


class ReadinessDependencies(BaseModel):
    database: str
    migrations: str
    kafka: str


class ReadinessResponse(BaseModel):
    status: str
    service: str
    dependencies: ReadinessDependencies
