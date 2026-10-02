from collections.abc import Callable
from typing import Any

import psycopg


class PostgresRepository:
    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    @classmethod
    def from_url(cls, database_url: str) -> "PostgresRepository":
        return cls(lambda: psycopg.connect(database_url))

    def summary(self) -> dict[str, float]:
        query = """
            WITH latest_metrics AS (
                SELECT
                    metric_name,
                    metric_value,
                    ROW_NUMBER() OVER (
                        PARTITION BY metric_name
                        ORDER BY window_start DESC
                    ) AS metric_rank
                FROM minute_metrics
                WHERE metric_name IN (%s, %s, %s)
            )
            SELECT metric_name, metric_value
            FROM latest_metrics
            WHERE metric_rank = 1
        """
        metric_names = ("orders", "revenue", "payment_success_rate")
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(query, metric_names)
            return {str(name): float(value) for name, value in cursor.fetchall()}

    def top_products(self, limit: int) -> list[dict[str, object]]:
        query = """
            SELECT product_id, order_count, revenue
            FROM product_window_metrics
            WHERE window_end = (SELECT MAX(window_end) FROM product_window_metrics)
            ORDER BY revenue DESC
            LIMIT %s
        """
        with self._connection_factory() as connection, connection.cursor() as cursor:
            cursor.execute(query, (limit,))
            return [
                {
                    "product_id": str(product_id),
                    "order_count": int(order_count),
                    "revenue": float(revenue),
                }
                for product_id, order_count, revenue in cursor.fetchall()
            ]
