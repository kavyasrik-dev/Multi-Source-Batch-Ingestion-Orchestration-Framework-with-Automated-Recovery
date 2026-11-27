"""Row-isolated normalization with rejected JSONL and staging Parquet publication."""

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from app.common.contracts.orders import OrderRecord, RecordValidationError
from app.common.storage.minio import MinioObjectStore, StoredObject
from app.common.storage.naming import DataZone, batch_prefix
from app.connectors.base import ExtractedBatch
from app.transformation.orders import OrderTransformer


class TransformationPublicationError(RuntimeError):
    """Accepted/rejected output could not be durably published."""


class StagingPublisher(Protocol):
    def publish(
        self,
        records: Sequence[OrderRecord],
        *,
        source: str,
        ingest_date: date,
        batch_id: UUID,
    ) -> tuple[StoredObject, ...]: ...


class QualityDecision(Protocol):
    @property
    def blocking_failed(self) -> bool: ...


class QualityGateRunner(Protocol):
    def validate_and_persist(
        self,
        records: Sequence[OrderRecord],
        *,
        metadata_batch_id: UUID,
        source: str,
        ingest_date: date,
        output_batch_id: UUID,
        existing_rejected_uris: Sequence[str] = (),
        attempt: int = 1,
    ) -> QualityDecision: ...


@dataclass(frozen=True, slots=True)
class RejectedRow:
    """Original extracted row plus canonical field failure reasons."""

    line_number: int
    values: Mapping[str, object]
    reasons: Mapping[str, str]


@dataclass(frozen=True, slots=True)
class TransformationResult:
    """Published outputs and exact input/accepted/rejected counts."""

    input_count: int
    accepted_count: int
    rejected_count: int
    accepted_records: tuple[OrderRecord, ...]
    rejected_rows: tuple[RejectedRow, ...]
    staging_objects: tuple[StoredObject, ...]
    rejected_object: StoredObject | None
    quality_report: QualityDecision | None = None
    publication_blocked: bool = False

    def __post_init__(self) -> None:
        if self.input_count != self.accepted_count + self.rejected_count:
            raise ValueError("input count must equal accepted plus rejected counts")
        if self.accepted_count != len(self.accepted_records):
            raise ValueError("accepted count does not match accepted records")
        if self.rejected_count != len(self.rejected_rows):
            raise ValueError("rejected count does not match rejected rows")


class TransformationPipeline:
    """Normalize an already-extracted batch and publish both outcome classes."""

    def __init__(
        self,
        transformer: OrderTransformer,
        store: MinioObjectStore,
        staging: StagingPublisher,
        quality_gate: QualityGateRunner | None = None,
    ) -> None:
        self.transformer = transformer
        self.store = store
        self.staging = staging
        self.quality_gate = quality_gate

    def process(
        self,
        batch: ExtractedBatch,
        *,
        source: str,
        ingested_at: datetime,
        metadata_batch_id: UUID | None = None,
        validation_attempt: int = 1,
    ) -> TransformationResult:
        """Transform row-by-row; extraction must have completed before this method is called."""
        if ingested_at.tzinfo is None or ingested_at.utcoffset() is None:
            raise ValueError("ingested_at must be timezone-aware")
        if self.quality_gate is not None and metadata_batch_id is None:
            raise ValueError("metadata_batch_id is required when a quality gate is configured")
        if (
            isinstance(validation_attempt, bool)
            or not isinstance(validation_attempt, int)
            or validation_attempt < 1
        ):
            raise ValueError("validation_attempt must be a positive integer")
        normalized_ingested_at = ingested_at.astimezone(UTC)
        ingest_date = normalized_ingested_at.date()
        accepted: list[OrderRecord] = []
        rejected: list[RejectedRow] = []
        for record in batch.records:
            try:
                accepted.append(
                    self.transformer.transform(
                        record,
                        batch_id=batch.batch_id,
                        ingested_at=normalized_ingested_at,
                    )
                )
            except RecordValidationError as exc:
                rejected.append(
                    RejectedRow(
                        line_number=record.line_number,
                        values=record.values,
                        reasons=dict(exc.errors),
                    )
                )
        try:
            rejected_object = self._publish_rejected(
                rejected,
                source=source,
                ingest_date=ingest_date,
                batch_id=batch.batch_id,
            )
            quality_report = None
            if self.quality_gate is not None:
                quality_report = self.quality_gate.validate_and_persist(
                    accepted,
                    metadata_batch_id=metadata_batch_id,
                    source=source,
                    ingest_date=ingest_date,
                    output_batch_id=batch.batch_id,
                    existing_rejected_uris=(
                        (rejected_object.uri,) if rejected_object is not None else ()
                    ),
                    attempt=validation_attempt,
                )
            publication_blocked = bool(
                quality_report is not None and quality_report.blocking_failed
            )
            staging_objects = (
                ()
                if publication_blocked
                else self.staging.publish(
                    accepted,
                    source=source,
                    ingest_date=ingest_date,
                    batch_id=batch.batch_id,
                )
            )
        except Exception as exc:
            raise TransformationPublicationError(
                f"failed to publish transformation outputs for batch {batch.batch_id}"
            ) from exc
        return TransformationResult(
            input_count=len(batch.records),
            accepted_count=len(accepted),
            rejected_count=len(rejected),
            accepted_records=tuple(accepted),
            rejected_rows=tuple(rejected),
            staging_objects=tuple(staging_objects),
            rejected_object=rejected_object,
            quality_report=quality_report,
            publication_blocked=publication_blocked,
        )

    def _publish_rejected(
        self,
        rejected: Sequence[RejectedRow],
        *,
        source: str,
        ingest_date: date,
        batch_id: UUID,
    ) -> StoredObject | None:
        if not rejected:
            return None
        lines = []
        for row in rejected:
            document = {
                "line_number": row.line_number,
                "reasons": dict(sorted(row.reasons.items())),
                "values": _json_value(dict(row.values)),
            }
            lines.append(json.dumps(document, sort_keys=True, separators=(",", ":")))
        payload = ("\n".join(lines) + "\n").encode()
        digest = hashlib.sha256(payload).hexdigest()[:16]
        key = f"{batch_prefix(source, ingest_date, batch_id)}/rejections/rejected-{digest}.jsonl"
        return self.store.upload_if_absent(
            DataZone.REJECTED.bucket,
            key,
            payload,
            content_type="application/x-ndjson",
        )


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value.astimezone(UTC).isoformat()
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, bytes):
        return {"encoding": "hex", "value": value.hex()}
    return {"type": type(value).__name__, "value": str(value)}
