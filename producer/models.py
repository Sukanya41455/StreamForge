from dataclasses import dataclass
from datetime import UTC, datetime


def _validate_common(event_id: str, amount: float, event_time: datetime) -> None:
    if not event_id:
        raise ValueError("event_id is required")
    if amount <= 0:
        raise ValueError("amount must be positive")
    if event_time.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")


def _utc_text(event_time: datetime) -> str:
    return event_time.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class OrderEvent:
    event_id: str
    order_id: str
    user_id: str
    product_id: str
    amount: float
    region: str
    event_time: datetime

    def __post_init__(self) -> None:
        _validate_common(self.event_id, self.amount, self.event_time)

    def to_record(self) -> dict[str, object]:
        return {
            "event_type": "order",
            "schema_version": 1,
            "event_id": self.event_id,
            "order_id": self.order_id,
            "user_id": self.user_id,
            "product_id": self.product_id,
            "amount": self.amount,
            "region": self.region,
            "event_time": _utc_text(self.event_time),
        }


@dataclass(frozen=True)
class PaymentEvent:
    event_id: str
    payment_id: str
    order_id: str
    status: str
    amount: float
    event_time: datetime

    def __post_init__(self) -> None:
        _validate_common(self.event_id, self.amount, self.event_time)
        if self.status not in {"succeeded", "failed"}:
            raise ValueError("status must be succeeded or failed")

    def to_record(self) -> dict[str, object]:
        return {
            "event_type": "payment",
            "schema_version": 1,
            "event_id": self.event_id,
            "payment_id": self.payment_id,
            "order_id": self.order_id,
            "status": self.status,
            "amount": self.amount,
            "event_time": _utc_text(self.event_time),
        }
