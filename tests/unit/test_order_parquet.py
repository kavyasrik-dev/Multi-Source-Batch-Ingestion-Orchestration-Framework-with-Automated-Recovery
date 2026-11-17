"""Canonical Parquet fidelity, schema, partition and file-sizing tests."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

pa = pytest.importorskip("pyarrow", reason="PyArrow is required for canonical Parquet tests")
pq = pytest.importorskip(
    "pyarrow.parquet",
    reason="PyArrow Parquet native support is required",
    exc_type=ImportError,
)

from app.common.contracts.orders import OrderRecord  # noqa: E402
from app.common.contracts.parquet import (  # noqa: E402
    ORDER_SCHEMA,
    ParquetSchemaError,
    partition_path,
    read_order_file,
    write_order_batch,
)

BATCH_ID = UUID("22222222-2222-4222-8222-222222222222")
INGEST_DATE = date(2026, 11, 17)


def order(index: int = 0, *, status: str | None = None, notes: str | None = None) -> OrderRecord:
    return OrderRecord(
        schema_version=1,
        order_id=UUID(f"11111111-1111-4111-8111-{index:012d}"),
        customer_id=f"customer-{index}",
        order_timestamp=datetime(2026, 11, 17, 12, tzinfo=UTC) + timedelta(microseconds=index),
        amount=Decimal("1234567890.12") + Decimal(index),
        currency="USD",
        status=status,
        source_system="api",
        ingested_at=datetime(2026, 11, 17, 13, tzinfo=UTC),
        batch_id=BATCH_ID,
        notes=notes,
    )


def test_round_trip_preserves_types_precision_schema_and_nulls(tmp_path):
    records = [order(), order(1, status="confirmed", notes="gift")]
    [path] = write_order_batch(records, tmp_path, ingest_date=INGEST_DATE, batch_id=BATCH_ID)
    assert read_order_file(path) == records
    parquet = pq.ParquetFile(path)
    assert parquet.schema_arrow.equals(ORDER_SCHEMA, check_metadata=True)
    assert parquet.metadata.row_group(0).column(4).compression == "ZSTD"
    assert "dataset=orders" in path.as_posix()
    assert "ingest_date=2026-11-17" in path.as_posix()
    assert f"batch_id={BATCH_ID}" in path.as_posix()


def test_deterministic_idempotent_name_and_content_change(tmp_path):
    records = [order()]
    first = write_order_batch(records, tmp_path, ingest_date=INGEST_DATE, batch_id=BATCH_ID)
    second = write_order_batch(records, tmp_path, ingest_date=INGEST_DATE, batch_id=BATCH_ID)
    changed = write_order_batch(
        [order(notes="changed")], tmp_path, ingest_date=INGEST_DATE, batch_id=BATCH_ID
    )
    assert first == second
    assert first[0].name != changed[0].name
    assert len(list(first[0].parent.glob("*.parquet"))) == 2


def test_empty_batch_writes_no_directory_or_file(tmp_path):
    assert write_order_batch([], tmp_path, ingest_date=INGEST_DATE, batch_id=BATCH_ID) == []
    assert not partition_path(tmp_path, "orders", INGEST_DATE, BATCH_ID).exists()


def test_small_remainder_is_merged_to_avoid_a_tiny_file(tmp_path):
    paths = write_order_batch(
        [order(i) for i in range(12)],
        tmp_path,
        ingest_date=INGEST_DATE,
        batch_id=BATCH_ID,
        target_rows_per_file=10,
        min_rows_per_file=4,
    )
    assert len(paths) == 1
    assert pq.ParquetFile(paths[0]).metadata.num_rows == 12


def test_large_remainder_becomes_a_second_file(tmp_path):
    paths = write_order_batch(
        [order(i) for i in range(15)],
        tmp_path,
        ingest_date=INGEST_DATE,
        batch_id=BATCH_ID,
        target_rows_per_file=10,
        min_rows_per_file=4,
    )
    assert [pq.ParquetFile(path).metadata.num_rows for path in paths] == [10, 5]


def test_schema_mismatch_is_rejected(tmp_path):
    path = tmp_path / "wrong.parquet"
    pq.write_table(pa.table({"order_id": ["x"]}), path)
    with pytest.raises(ParquetSchemaError, match="canonical orders schema v1"):
        read_order_file(path)


def test_partition_and_batch_invariants_are_enforced(tmp_path):
    with pytest.raises(ValueError, match="dataset must match"):
        partition_path(tmp_path, "../orders", INGEST_DATE, BATCH_ID)
    with pytest.raises(ValueError, match="batch_id"):
        write_order_batch([order()], tmp_path, ingest_date=INGEST_DATE, batch_id=UUID(int=3))
    with pytest.raises(ValueError, match="ingest_date"):
        write_order_batch([order()], tmp_path, ingest_date=date(2026, 11, 18), batch_id=BATCH_ID)


def test_file_size_configuration_is_validated(tmp_path):
    with pytest.raises(ValueError, match="min_rows_per_file"):
        write_order_batch(
            [order()],
            tmp_path,
            ingest_date=INGEST_DATE,
            batch_id=BATCH_ID,
            target_rows_per_file=10,
            min_rows_per_file=11,
        )
