"""Deterministic canonical Parquet publication to the staging object zone."""

import hashlib
import json
from collections.abc import Sequence
from datetime import date
from uuid import UUID

from app.common.contracts.layout import plan_file_sizes
from app.common.contracts.orders import OrderRecord
from app.common.storage.minio import MinioObjectStore, StoredObject
from app.common.storage.naming import DataZone, data_object_key


def _record_payload(records: Sequence[OrderRecord]) -> bytes:
    rows = []
    for record in records:
        row = record.as_dict()
        row["order_timestamp"] = record.order_timestamp.isoformat(timespec="microseconds")
        row["ingested_at"] = record.ingested_at.isoformat(timespec="microseconds")
        row["amount"] = format(record.amount, ".2f")
        rows.append(row)
    return json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()


class MinioParquetPublisher:
    """Write deterministic Zstandard Parquet parts directly to MinIO staging."""

    def __init__(
        self,
        store: MinioObjectStore,
        *,
        target_rows_per_file: int = 100_000,
        min_rows_per_file: int = 10_000,
    ) -> None:
        plan_file_sizes(0, target_rows_per_file, min_rows_per_file)
        self.store = store
        self.target_rows_per_file = target_rows_per_file
        self.min_rows_per_file = min_rows_per_file

    def publish(
        self,
        records: Sequence[OrderRecord],
        *,
        source: str,
        ingest_date: date,
        batch_id: UUID,
    ) -> tuple[StoredObject, ...]:
        """Serialize exact-schema Parquet and idempotently upload deterministic parts."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from app.common.contracts.parquet import orders_to_table

        records = tuple(records)
        for index, record in enumerate(records):
            if not isinstance(record, OrderRecord):
                raise TypeError(f"records[{index}] must be an OrderRecord")
            if record.batch_id != batch_id:
                raise ValueError(f"records[{index}].batch_id does not match output batch")
            if record.ingested_at.date() != ingest_date:
                raise ValueError(f"records[{index}].ingested_at does not match ingest_date")
        sizes = plan_file_sizes(len(records), self.target_rows_per_file, self.min_rows_per_file)
        stored: list[StoredObject] = []
        offset = 0
        for part_number, size in enumerate(sizes):
            chunk = records[offset : offset + size]
            offset += size
            digest = hashlib.sha256(_record_payload(chunk)).hexdigest()[:16]
            filename = f"part-{part_number:05d}-{digest}.parquet"
            key = data_object_key(source, ingest_date, batch_id, filename)
            sink = pa.BufferOutputStream()
            pq.write_table(
                orders_to_table(chunk),
                sink,
                compression="zstd",
                compression_level=6,
                use_dictionary=["currency", "status", "source_system"],
                write_statistics=True,
            )
            payload = sink.getvalue().to_pybytes()
            stored.append(
                self.store.upload_if_absent(
                    DataZone.STAGING.bucket,
                    key,
                    payload,
                    content_type="application/vnd.apache.parquet",
                )
            )
        return tuple(stored)
