from collections.abc import Callable
from os import getenv
from typing import Protocol

from fastapi import FastAPI, Query
from prometheus_client import CollectorRegistry, Counter, Histogram, make_asgi_app

from api.repository import PostgresRepository


class AnalyticsRepository(Protocol):
    def summary(self) -> dict[str, float]: ...

    def top_products(self, limit: int) -> list[dict[str, object]]: ...


class EmptyRepository:
    def summary(self) -> dict[str, float]:
        return {}

    def top_products(self, limit: int) -> list[dict[str, object]]:
        return []


def create_app(repository: AnalyticsRepository) -> FastAPI:
    app = FastAPI(title="StreamForge Analytics API")
    registry = CollectorRegistry()
    requests = Counter(
        "streamforge_http_requests",
        "HTTP responses served by the analytics API.",
        ["path", "status"],
        registry=registry,
    )
    latency = Histogram(
        "streamforge_http_request_duration_seconds",
        "HTTP request duration for the analytics API.",
        ["path"],
        registry=registry,
    )

    @app.middleware("http")
    async def record_request(request, call_next: Callable):
        with latency.labels(request.url.path).time():
            response = await call_next(request)
        requests.labels(request.url.path, response.status_code).inc()
        return response

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/analytics/summary")
    def summary() -> dict[str, float]:
        values = repository.summary()
        return {
            "orders": float(values.get("orders", 0.0)),
            "revenue": float(values.get("revenue", 0.0)),
            "payment_success_rate": float(values.get("payment_success_rate", 0.0)),
        }

    @app.get("/analytics/top-products")
    def top_products(limit: int = Query(default=10, ge=1, le=100)) -> list[dict[str, object]]:
        return repository.top_products(limit)

    app.mount("/metrics", make_asgi_app(registry=registry))
    return app


app = create_app(
    PostgresRepository.from_url(
        getenv("DATABASE_URL", "postgresql://streamforge:streamforge@postgres:5432/streamforge")
    )
)
