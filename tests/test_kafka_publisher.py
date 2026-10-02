from datetime import UTC, datetime
import json

from producer.kafka_publisher import (
    ORDERS_TOPIC,
    PAYMENTS_TOPIC,
    publish_events,
)
from producer.models import OrderEvent, PaymentEvent


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.flushed = False

    def send(self, topic: str, value: bytes, key: bytes) -> None:
        self.sent.append((topic, value, key))

    def flush(self) -> None:
        self.flushed = True


def test_publish_events_routes_records_to_versioned_topics_and_flushes() -> None:
    events = [
        OrderEvent(
            event_id="evt-order",
            order_id="ord-1",
            user_id="usr-1",
            product_id="prd-1",
            amount=19.99,
            region="texas",
            event_time=datetime(2026, 10, 1, tzinfo=UTC),
        ),
        PaymentEvent(
            event_id="evt-payment",
            payment_id="pay-1",
            order_id="ord-1",
            status="succeeded",
            amount=19.99,
            event_time=datetime(2026, 10, 1, tzinfo=UTC),
        ),
    ]
    producer = FakeProducer()

    stats = publish_events(events, producer)

    assert stats == {"orders": 1, "payments": 1}
    assert producer.flushed is True
    assert producer.sent[0][0] == ORDERS_TOPIC
    assert producer.sent[0][2] == b"ord-1"
    assert json.loads(producer.sent[0][1]) == events[0].to_record()
    assert producer.sent[1][0] == PAYMENTS_TOPIC
    assert producer.sent[1][2] == b"ord-1"
    assert json.loads(producer.sent[1][1]) == events[1].to_record()
