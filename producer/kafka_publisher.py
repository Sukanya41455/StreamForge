import json
from collections.abc import Iterable
from typing import Protocol

from producer.generator import Event
from producer.models import OrderEvent, PaymentEvent

ORDERS_TOPIC = "streamforge.orders.v1"
PAYMENTS_TOPIC = "streamforge.payments.v1"


class KafkaProducer(Protocol):
    def send(self, topic: str, value: bytes, key: bytes) -> None: ...

    def flush(self) -> None: ...


def publish_events(events: Iterable[Event], producer: KafkaProducer) -> dict[str, int]:
    stats = {"orders": 0, "payments": 0}
    for event in events:
        if isinstance(event, OrderEvent):
            topic = ORDERS_TOPIC
            stats["orders"] += 1
        elif isinstance(event, PaymentEvent):
            topic = PAYMENTS_TOPIC
            stats["payments"] += 1
        else:
            raise TypeError(f"unsupported event type: {type(event).__name__}")

        producer.send(
            topic,
            value=json.dumps(event.to_record()).encode(),
            key=event.order_id.encode(),
        )
    producer.flush()
    return stats
