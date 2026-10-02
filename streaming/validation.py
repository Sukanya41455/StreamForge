from collections.abc import Mapping
from datetime import datetime
from typing import Any


def _as_dict(record: Mapping[str, Any] | Any | None) -> dict[str, Any]:
    if record is None:
        return {}
    if hasattr(record, "asDict"):
        return record.asDict(recursive=True)
    return dict(record)


def _validate_common(record: Mapping[str, Any] | Any | None, required: tuple[str, ...]) -> str | None:
    value = _as_dict(record)
    if value.get("schema_version") != 1:
        return "unsupported schema version"
    for field in required:
        if not value.get(field):
            return f"missing {field}"
    if float(value["amount"]) <= 0:
        return "amount must be positive"
    try:
        datetime.fromisoformat(str(value["event_time"]).replace("Z", "+00:00"))
    except ValueError:
        return "invalid event_time"
    return None


def validate_order_record(record: Mapping[str, Any] | Any | None) -> str | None:
    return _validate_common(
        record,
        ("event_id", "order_id", "user_id", "product_id", "amount", "region", "event_time"),
    )


def validate_payment_record(record: Mapping[str, Any] | Any | None) -> str | None:
    error = _validate_common(
        record,
        ("event_id", "payment_id", "order_id", "status", "amount", "event_time"),
    )
    if error:
        return error
    if _as_dict(record)["status"] not in {"succeeded", "failed"}:
        return "invalid payment status"
    return None
