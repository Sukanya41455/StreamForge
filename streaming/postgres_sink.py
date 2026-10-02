from collections.abc import Iterable
from typing import Any

import psycopg


def upsert_minute_metrics(rows: Iterable[Any], database_url: str) -> None:
    values: list[tuple[object, str, float]] = []
    for row in rows:
        value = row.asDict(recursive=True)
        for name in ("orders", "revenue", "payments", "payment_success_rate"):
            if name in value and value[name] is not None:
                values.append((value["window_start"], name, float(value[name])))
    if not values:
        return
    query = """
        INSERT INTO minute_metrics (window_start, metric_name, metric_value)
        VALUES (%s, %s, %s)
        ON CONFLICT (window_start, metric_name)
        DO UPDATE SET metric_value = EXCLUDED.metric_value
    """
    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.executemany(query, values)


def upsert_product_metrics(rows: Iterable[Any], database_url: str) -> None:
    values = [
        (
            row["window_start"],
            row["window_end"],
            row["product_id"],
            int(row["order_count"]),
            float(row["revenue"]),
        )
        for row in rows
    ]
    if not values:
        return
    query = """
        INSERT INTO product_window_metrics (window_start, window_end, product_id, order_count, revenue)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (window_start, window_end, product_id)
        DO UPDATE SET order_count = EXCLUDED.order_count, revenue = EXCLUDED.revenue
    """
    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.executemany(query, values)
