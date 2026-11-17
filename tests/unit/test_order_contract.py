"""Canonical order parsing and conversion policy tests."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from app.common.contracts.orders import RecordValidationError, parse_order, parse_orders

ORDER_ID = "11111111-1111-4111-8111-111111111111"
BATCH_ID = "22222222-2222-4222-8222-222222222222"


def valid_order(**changes):
    row = {
        "schema_version": 1,
        "order_id": ORDER_ID,
        "customer_id": " customer-7 ",
        "order_timestamp": "2026-11-17T08:30:00-05:00",
        "amount": "19.9",
        "currency": "usd",
        "status": None,
        "source_system": " api ",
        "ingested_at": "2026-11-17T13:31:00Z",
        "batch_id": BATCH_ID,
        "notes": None,
    }
    row.update(changes)
    return row


def test_parse_normalizes_explicit_values_without_loss():
    record = parse_order(valid_order())
    assert record.order_id == UUID(ORDER_ID)
    assert record.customer_id == "customer-7"
    assert record.order_timestamp == datetime(2026, 11, 17, 13, 30, tzinfo=UTC)
    assert record.ingested_at == datetime(2026, 11, 17, 13, 31, tzinfo=UTC)
    assert record.amount == Decimal("19.90")
    assert record.currency == "USD"
    assert record.status is None and record.notes is None


@pytest.mark.parametrize(
    ("changes", "field", "message"),
    [
        ({"order_timestamp": "2026-11-17T08:30:00"}, "order_timestamp", "timezone"),
        ({"amount": 1.1}, "amount", "floats are rejected"),
        ({"amount": "1.001"}, "amount", "2 fractional digits"),
        ({"amount": True}, "amount", "floats are rejected"),
        ({"currency": "US"}, "currency", "3-letter"),
        ({"schema_version": 2}, "schema_version", "must equal 1"),
        ({"schema_version": 1.0}, "schema_version", "must equal 1"),
        (
            {"order_id": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"},
            "order_id",
            "canonical",
        ),
        ({"status": "unknown"}, "status", "must be null or one of"),
        ({"order_id": "not-a-uuid"}, "order_id", "valid UUID"),
        ({"unexpected": "value"}, "record", "unknown fields"),
    ],
)
def test_invalid_conversions_are_actionable(changes, field, message):
    with pytest.raises(RecordValidationError) as caught:
        parse_order(valid_order(**changes))
    assert message in caught.value.errors[field]


def test_batch_errors_include_record_indexes_and_all_invalid_rows():
    with pytest.raises(RecordValidationError) as caught:
        parse_orders([valid_order(amount="x"), valid_order(currency=None)])
    assert "records[0].amount" in caught.value.errors
    assert "records[1].currency" in caught.value.errors


def test_missing_required_fields_and_non_mapping_are_clear():
    with pytest.raises(RecordValidationError) as caught:
        parse_order({"schema_version": 1})
    assert "batch_id" in caught.value.errors
    with pytest.raises(RecordValidationError, match="object/mapping"):
        parse_order([])
