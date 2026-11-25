"""Exact-schema staging Parquet publication test when native PyArrow is available."""

import hashlib
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

pa = pytest.importorskip("pyarrow", reason="PyArrow is required for staging Parquet tests")
pq = pytest.importorskip(
    "pyarrow.parquet",
    reason="PyArrow Parquet native support is required",
    exc_type=ImportError,
)

from app.common.contracts.orders import OrderRecord  # noqa: E402
from app.common.contracts.parquet import ORDER_SCHEMA  # noqa: E402
from app.common.storage.minio import StoredObject  # noqa: E402
from app.transformation.publication import MinioParquetPublisher  # noqa: E402

BATCH_ID = UUID("22222222-2222-4222-8222-222222222222")


class MemoryStore:
    def __init__(self):
        self.objects = {}

    def upload_if_absent(self, bucket, key, data, *, content_type):
        existing = self.objects.setdefault((bucket, key), (data, content_type))
        assert existing == (data, content_type)
        return StoredObject(bucket, key, hashlib.sha256(data).hexdigest(), len(data), None)


def order():
    return OrderRecord(
        schema_version=1,
        order_id=UUID("11111111-1111-4111-8111-111111111111"),
        customer_id="customer-1",
        order_timestamp=datetime(2026, 11, 25, 14, tzinfo=UTC),
        amount=Decimal("19.90"),
        currency="USD",
        status="confirmed",
        source_system="legacy-orders",
        ingested_at=datetime(2026, 11, 25, 15, tzinfo=UTC),
        batch_id=BATCH_ID,
    )


def test_staging_parquet_has_canonical_schema_compression_and_stable_key():
    store = MemoryStore()
    publisher = MinioParquetPublisher(store, target_rows_per_file=10, min_rows_per_file=1)
    first = publisher.publish(
        [order()], source="legacy-orders", ingest_date=date(2026, 11, 25), batch_id=BATCH_ID
    )
    second = publisher.publish(
        [order()], source="legacy-orders", ingest_date=date(2026, 11, 25), batch_id=BATCH_ID
    )
    assert first == second
    assert len(store.objects) == 1
    [(bucket, key), (payload, content_type)] = list(store.objects.items())[0]
    assert bucket == "staging"
    assert "source=legacy-orders/logical_date=2026-11-25" in key
    assert content_type == "application/vnd.apache.parquet"
    parquet = pq.ParquetFile(pa.BufferReader(payload))
    assert parquet.schema_arrow.equals(ORDER_SCHEMA, check_metadata=True)
    assert parquet.metadata.num_rows == 1
    assert parquet.metadata.row_group(0).column(4).compression == "ZSTD"
