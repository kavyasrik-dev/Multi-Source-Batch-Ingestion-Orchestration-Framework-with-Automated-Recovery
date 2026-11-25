"""Canonical mapping, rejection, publication counts, and deterministic rerun tests."""

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import MappingProxyType
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from app.common.storage.minio import StoredObject
from app.connectors.base import ExtractedBatch, SourceMetadata, SourceObject, SourceRecord
from app.transformation.orders import OrderMapping, OrderTransformer
from app.transformation.pipeline import (
    TransformationPipeline,
    TransformationPublicationError,
)

BATCH_ID = UUID("22222222-2222-4222-8222-222222222222")
INGESTED_AT = datetime(2026, 11, 25, 14, 30, tzinfo=UTC)


class MemoryStore:
    def __init__(self):
        self.objects = {}

    def upload_if_absent(self, bucket, key, data, *, content_type):
        location = (bucket, key)
        existing = self.objects.setdefault(location, (data, content_type))
        assert existing == (data, content_type)
        return StoredObject(bucket, key, hashlib.sha256(data).hexdigest(), len(data), None)


class FakeStagingPublisher:
    def __init__(self):
        self.calls = []

    def publish(self, records, *, source, ingest_date, batch_id):
        records = tuple(records)
        self.calls.append((records, source, ingest_date, batch_id))
        if not records:
            return ()
        payload = json.dumps(
            [
                {
                    **record.as_dict(),
                    "order_id": str(record.order_id),
                    "order_timestamp": record.order_timestamp.isoformat(),
                    "amount": str(record.amount),
                    "ingested_at": record.ingested_at.isoformat(),
                    "batch_id": str(record.batch_id),
                }
                for record in records
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        digest = hashlib.sha256(payload).hexdigest()
        key = f"staging/{source}/{ingest_date}/{batch_id}/{digest}.parquet"
        return (StoredObject("staging", key, digest, len(payload), None),)


def mapping():
    return OrderMapping(
        source_system="legacy-orders",
        order_id_field="order_no",
        customer_id_field="buyer",
        order_timestamp_field="placed_at",
        amount_field="total",
        currency_field="currency_name",
        status_field="state",
        notes_field="memo",
        order_id_mode="uuid5",
        timestamp_formats=("%m/%d/%Y %H:%M:%S %z",),
        status_aliases={"PAID": "confirmed", "SHIPPED": "fulfilled"},
        currency_aliases={"dollars": "USD", "euros": "EUR"},
    )


def source_row(**changes):
    values = {
        "order_no": " ORD-1001 ",
        "buyer": " customer-7 ",
        "placed_at": "11/25/2026 09:15:00 -0500",
        "total": "19.9",
        "currency_name": "DOLLARS",
        "state": "Paid",
        "memo": "gift",
    }
    values.update(changes)
    return values


def extracted_batch(rows):
    source = SourceObject("file:///orders.csv", "orders.csv")
    metadata = SourceMetadata(
        identity=uuid5(NAMESPACE_URL, "orders.csv"),
        checksum_sha256="a" * 64,
        byte_count=100,
        modified_at=INGESTED_AT,
        media_type="text/csv",
    )
    records = tuple(
        SourceRecord(MappingProxyType(row), line_number=index + 2) for index, row in enumerate(rows)
    )
    return ExtractedBatch(BATCH_ID, source, metadata, 0, records)


def pipeline(store=None, publisher=None):
    return TransformationPipeline(
        OrderTransformer(mapping()),
        store or MemoryStore(),
        publisher or FakeStagingPublisher(),
    )


def test_mapping_normalizes_identifiers_timestamp_decimal_and_categories():
    store = MemoryStore()
    publisher = FakeStagingPublisher()
    result = pipeline(store, publisher).process(
        extracted_batch([source_row()]),
        source="legacy-orders",
        ingested_at=INGESTED_AT,
    )
    assert (result.input_count, result.accepted_count, result.rejected_count) == (1, 1, 0)
    [record] = result.accepted_records
    assert record.order_id == uuid5(NAMESPACE_URL, "legacy-orders\0ORD-1001")
    assert record.customer_id == "customer-7"
    assert record.order_timestamp == datetime(2026, 11, 25, 14, 15, tzinfo=UTC)
    assert record.amount == Decimal("19.90")
    assert record.currency == "USD"
    assert record.status == "confirmed"
    assert record.ingested_at == INGESTED_AT
    assert record.batch_id == BATCH_ID
    assert result.rejected_object is None
    assert len(result.staging_objects) == 1


def test_malformed_rows_are_isolated_with_actionable_reasons_and_raw_values():
    store = MemoryStore()
    publisher = FakeStagingPublisher()
    rows = [
        source_row(total="1.999"),
        source_row(order_no="ORD-2", placed_at="not-a-date"),
        source_row(order_no="ORD-3", state="mystery"),
        source_row(order_no="ORD-4", total="20.00"),
    ]
    result = pipeline(store, publisher).process(
        extracted_batch(rows), source="legacy-orders", ingested_at=INGESTED_AT
    )
    assert (result.input_count, result.accepted_count, result.rejected_count) == (4, 1, 3)
    assert "fractional digits" in result.rejected_rows[0].reasons["amount"]
    assert "configured format" in result.rejected_rows[1].reasons["order_timestamp"]
    assert "one of" in result.rejected_rows[2].reasons["status"]
    assert result.rejected_object is not None
    assert result.rejected_object.bucket == "rejected"
    [(location, (payload, content_type))] = list(store.objects.items())
    assert location[0] == "rejected"
    assert content_type == "application/x-ndjson"
    documents = [json.loads(line) for line in payload.splitlines()]
    assert [document["line_number"] for document in documents] == [2, 3, 4]
    assert documents[0]["values"]["total"] == "1.999"
    assert len(publisher.calls[0][0]) == 1


def test_deterministic_rerun_reuses_rejection_and_staging_identities():
    store = MemoryStore()
    publisher = FakeStagingPublisher()
    batch = extracted_batch([source_row(), source_row(order_no="ORD-2", total="bad")])
    first = pipeline(store, publisher).process(
        batch, source="legacy-orders", ingested_at=INGESTED_AT
    )
    second = pipeline(store, publisher).process(
        batch, source="legacy-orders", ingested_at=INGESTED_AT
    )
    assert first.accepted_records == second.accepted_records
    assert first.staging_objects == second.staging_objects
    assert first.rejected_object == second.rejected_object
    assert len(store.objects) == 1


def test_missing_fields_and_bad_source_identifier_are_rejected_not_extraction_errors():
    rows = [source_row(order_no=[]), source_row(order_no="ORD-2")]
    del rows[1]["buyer"]
    result = pipeline().process(
        extracted_batch(rows), source="legacy-orders", ingested_at=INGESTED_AT
    )
    assert result.accepted_count == 0
    assert "uuid5 source identifier" in result.rejected_rows[0].reasons["order_id"]
    assert "source field 'buyer' is missing" in result.rejected_rows[1].reasons["customer_id"]


def test_publication_failure_is_distinct_from_row_transformation_failure():
    class BrokenPublisher:
        def publish(self, records, **kwargs):
            raise OSError("staging unavailable")

    with pytest.raises(TransformationPublicationError, match="failed to publish") as caught:
        pipeline(publisher=BrokenPublisher()).process(
            extracted_batch([source_row()]),
            source="legacy-orders",
            ingested_at=INGESTED_AT,
        )
    assert isinstance(caught.value.__cause__, OSError)


def test_ingested_at_must_be_explicit_and_timezone_aware():
    with pytest.raises(ValueError, match="timezone-aware"):
        pipeline().process(
            extracted_batch([source_row()]),
            source="legacy-orders",
            ingested_at=datetime(2026, 11, 25),
        )


def test_mapping_rules_are_validated_before_processing():
    with pytest.raises(ValueError, match="canonical order statuses"):
        OrderMapping(
            source_system="x",
            order_id_field="id",
            customer_id_field="customer",
            order_timestamp_field="timestamp",
            amount_field="amount",
            currency_field="currency",
            status_aliases={"done": "not-canonical"},
        )
    with pytest.raises(ValueError, match="order_id_mode"):
        OrderMapping(
            source_system="x",
            order_id_field="id",
            customer_id_field="customer",
            order_timestamp_field="timestamp",
            amount_field="amount",
            currency_field="currency",
            order_id_mode="random",
        )
