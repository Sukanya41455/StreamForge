from datetime import UTC, datetime

import pytest

from producer.models import OrderEvent, PaymentEvent


def test_order_event_serializes_the_versioned_kafka_contract() -> None:
    event = OrderEvent(
        event_id="evt-1",
        order_id="ord-1",
        user_id="usr-1",
        product_id="prd-1",
        amount=19.99,
        region="texas",
        event_time=datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
    )

    assert event.to_record() == {
        "event_type": "order",
        "schema_version": 1,
        "event_id": "evt-1",
        "order_id": "ord-1",
        "user_id": "usr-1",
        "product_id": "prd-1",
        "amount": 19.99,
        "region": "texas",
        "event_time": "2026-10-01T12:00:00Z",
    }


def test_order_event_rejects_a_negative_amount() -> None:
    with pytest.raises(ValueError, match="amount must be positive"):
        OrderEvent(
            event_id="evt-1",
            order_id="ord-1",
            user_id="usr-1",
            product_id="prd-1",
            amount=-1,
            region="texas",
            event_time=datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
        )


def test_payment_event_serializes_status_and_utc_time() -> None:
    event = PaymentEvent(
        event_id="pay-1",
        payment_id="payment-1",
        order_id="ord-1",
        status="succeeded",
        amount=19.99,
        event_time=datetime(2026, 10, 1, 12, 0, tzinfo=UTC),
    )

    assert event.to_record()["status"] == "succeeded"
    assert event.to_record()["event_time"].endswith("Z")
