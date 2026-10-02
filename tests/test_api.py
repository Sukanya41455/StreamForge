from fastapi.testclient import TestClient

from api.main import create_app


class FakeRepository:
    def __init__(self, summary: dict[str, float], products: list[dict[str, object]]) -> None:
        self.summary_value = summary
        self.products_value = products
        self.requested_limit: int | None = None

    def summary(self) -> dict[str, float]:
        return self.summary_value

    def top_products(self, limit: int) -> list[dict[str, object]]:
        self.requested_limit = limit
        return self.products_value[:limit]


def test_summary_returns_persisted_analytics() -> None:
    repository = FakeRepository(
        {"orders": 12.0, "revenue": 340.5, "payment_success_rate": 91.7}, []
    )
    client = TestClient(create_app(repository))

    response = client.get("/analytics/summary")

    assert response.status_code == 200
    assert response.json() == {
        "orders": 12.0,
        "revenue": 340.5,
        "payment_success_rate": 91.7,
    }


def test_summary_returns_zeroes_when_no_metrics_exist() -> None:
    client = TestClient(create_app(FakeRepository({}, [])))

    response = client.get("/analytics/summary")

    assert response.status_code == 200
    assert response.json() == {
        "orders": 0.0,
        "revenue": 0.0,
        "payment_success_rate": 0.0,
    }


def test_top_products_validates_limit_and_uses_repository() -> None:
    repository = FakeRepository(
        {},
        [
            {"product_id": "prd-1", "order_count": 5, "revenue": 20.0},
            {"product_id": "prd-2", "order_count": 3, "revenue": 10.0},
        ],
    )
    client = TestClient(create_app(repository))

    response = client.get("/analytics/top-products?limit=1")

    assert response.status_code == 200
    assert response.json() == [
        {"product_id": "prd-1", "order_count": 5, "revenue": 20.0}
    ]
    assert repository.requested_limit == 1
    assert client.get("/analytics/top-products?limit=0").status_code == 422


def test_health_and_metrics_are_exposed() -> None:
    client = TestClient(create_app(FakeRepository({}, [])))

    assert client.get("/healthz").json() == {"status": "ok"}
    assert "streamforge_http_requests_total" in client.get("/metrics").text
