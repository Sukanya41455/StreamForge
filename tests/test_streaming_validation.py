from streaming.validation import validate_order_record, validate_payment_record


def test_valid_order_record_has_no_validation_error() -> None:
    record = {
        "schema_version": 1,
        "event_id": "evt-1",
        "order_id": "ord-1",
        "user_id": "usr-1",
        "product_id": "prd-1",
        "amount": 10.0,
        "region": "texas",
        "event_time": "2026-10-01T12:00:00Z",
    }

    assert validate_order_record(record) is None


def test_order_record_with_a_missing_event_id_is_rejected() -> None:
    record = {
        "schema_version": 1,
        "order_id": "ord-1",
        "user_id": "usr-1",
        "product_id": "prd-1",
        "amount": 10.0,
        "region": "texas",
        "event_time": "2026-10-01T12:00:00Z",
    }

    assert validate_order_record(record) == "missing event_id"


def test_payment_record_rejects_unknown_status() -> None:
    record = {
        "schema_version": 1,
        "event_id": "evt-1",
        "payment_id": "pay-1",
        "order_id": "ord-1",
        "status": "pending",
        "amount": 10.0,
        "event_time": "2026-10-01T12:00:00Z",
    }

    assert validate_payment_record(record) == "invalid payment status"
