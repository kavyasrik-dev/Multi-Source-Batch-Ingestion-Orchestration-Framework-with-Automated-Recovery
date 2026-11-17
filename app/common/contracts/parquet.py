"""Schema-preserving Parquet storage for canonical order batches."""

import hashlib
import json
import os
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from app.common.contracts.layout import partition_path, plan_file_sizes
from app.common.contracts.orders import (
    ORDER_SCHEMA_VERSION,
    OrderRecord,
    RecordValidationError,
    parse_order,
)

ORDER_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("customer_id", pa.string(), nullable=False),
        pa.field("order_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("amount", pa.decimal128(18, 2), nullable=False),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("status", pa.string(), nullable=True),
        pa.field("source_system", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("notes", pa.string(), nullable=True),
    ],
    metadata={b"dataset": b"orders", b"schema_version": str(ORDER_SCHEMA_VERSION).encode()},
)


class ParquetSchemaError(ValueError):
    """A Parquet file does not implement the exact canonical schema."""


def orders_to_table(records: Sequence[OrderRecord]) -> pa.Table:
    """Construct an exact-schema table; empty input retains every field and metadata."""
    return pa.Table.from_pylist([record.as_dict() for record in records], schema=ORDER_SCHEMA)


def _fingerprint(records: Sequence[OrderRecord]) -> str:
    payload = []
    for record in records:
        row = record.as_dict()
        row["order_timestamp"] = record.order_timestamp.isoformat(timespec="microseconds")
        row["ingested_at"] = record.ingested_at.isoformat(timespec="microseconds")
        row["amount"] = format(record.amount, ".2f")
        payload.append(row)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()[:16]


def write_order_batch(
    records: Sequence[OrderRecord],
    root: Path,
    *,
    ingest_date: date,
    batch_id: UUID,
    target_rows_per_file: int = 100_000,
    min_rows_per_file: int = 10_000,
) -> list[Path]:
    """Write deterministic, atomic Zstandard Parquet parts; empty batches write nothing."""
    records = tuple(records)
    for index, record in enumerate(records):
        if not isinstance(record, OrderRecord):
            raise TypeError(f"records[{index}] must be an OrderRecord")
        try:
            validated = parse_order(record.as_dict())
        except RecordValidationError as exc:
            raise ValueError(f"records[{index}] is not canonical: {exc}") from exc
        if validated != record:
            raise ValueError(f"records[{index}] contains non-normalized canonical values")
        if record.batch_id != batch_id:
            raise ValueError(f"records[{index}].batch_id does not match partition batch_id")
        if record.ingested_at.date() != ingest_date:
            raise ValueError(f"records[{index}].ingested_at does not match partition ingest_date")
    sizes = plan_file_sizes(len(records), target_rows_per_file, min_rows_per_file)
    directory = partition_path(root, "orders", ingest_date, batch_id)
    paths, offset = [], 0
    for part, size in enumerate(sizes):
        chunk = records[offset : offset + size]
        offset += size
        path = directory / f"part-{part:05d}-{_fingerprint(chunk)}.parquet"
        if path.exists():
            if read_order_file(path) != list(chunk):
                raise FileExistsError(f"deterministic output path has different content: {path}")
            paths.append(path)
            continue
        directory.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(f".tmp-{os.getpid()}-{uuid4().hex}")
        try:
            pq.write_table(
                orders_to_table(chunk),
                temporary,
                compression="zstd",
                compression_level=6,
                use_dictionary=["currency", "status", "source_system"],
                write_statistics=True,
            )
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
        paths.append(path)
    return paths


def read_order_file(path: Path) -> list[OrderRecord]:
    """Read one file only when its Arrow schema exactly matches canonical orders v1."""
    # ParquetFile avoids dataset partition-column inference from the parent path.
    parquet = pq.ParquetFile(path)
    if not parquet.schema_arrow.equals(ORDER_SCHEMA, check_metadata=True):
        raise ParquetSchemaError(
            f"{path}: expected canonical orders schema v{ORDER_SCHEMA_VERSION}"
        )
    table = parquet.read()
    return [parse_order(row) for row in table.to_pylist()]
