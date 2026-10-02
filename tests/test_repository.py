import sqlite3

from api.repository import PostgresRepository


class FakeCursor:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.executed: tuple[str, tuple[object, ...]] | None = None

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.executed = (query, params)

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self.cursor_value = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> FakeCursor:
        return self.cursor_value


class SqliteCursor:
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        self._cursor.close()

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self._cursor.execute(query.replace("%s", "?"), params)

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._cursor.fetchall()


class SqliteConnection:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def cursor(self) -> SqliteCursor:
        return SqliteCursor(self._connection.cursor())


def test_summary_maps_latest_metric_rows_to_api_fields() -> None:
    cursor = FakeCursor(
        [("orders", 12.0), ("revenue", 340.5), ("payment_success_rate", 91.7)]
    )
    repository = PostgresRepository(lambda: FakeConnection(cursor))

    assert repository.summary() == {
        "orders": 12.0,
        "revenue": 340.5,
        "payment_success_rate": 91.7,
    }
    assert cursor.executed is not None
    assert cursor.executed[1] == ("orders", "revenue", "payment_success_rate")


def test_summary_uses_newest_value_for_each_metric() -> None:
    connection = sqlite3.connect(":memory:")
    connection.execute(
        """
        CREATE TABLE minute_metrics (
            window_start TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            metric_value REAL NOT NULL
        )
        """
    )
    connection.executemany(
        "INSERT INTO minute_metrics VALUES (?, ?, ?)",
        [
            ("2026-10-01T00:58:00Z", "orders", 30.0),
            ("2026-10-01T00:59:00Z", "orders", 23.0),
            ("2026-10-01T00:58:00Z", "revenue", 3278.68),
            ("2026-10-01T00:59:00Z", "revenue", 2299.4),
            ("2026-10-01T00:59:00Z", "payment_success_rate", 80.0),
            ("2026-10-01T01:00:00Z", "payment_success_rate", 100.0),
        ],
    )
    repository = PostgresRepository(lambda: SqliteConnection(connection))

    assert repository.summary() == {
        "orders": 23.0,
        "revenue": 2299.4,
        "payment_success_rate": 100.0,
    }


def test_top_products_limits_sorted_query_results() -> None:
    cursor = FakeCursor([("prd-1", 5, 20.0)])
    repository = PostgresRepository(lambda: FakeConnection(cursor))

    assert repository.top_products(3) == [
        {"product_id": "prd-1", "order_count": 5, "revenue": 20.0}
    ]
    assert cursor.executed is not None
    assert cursor.executed[1] == (3,)
