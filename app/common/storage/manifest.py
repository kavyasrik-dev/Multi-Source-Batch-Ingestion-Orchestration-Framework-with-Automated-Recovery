"""Canonical batch manifest serialization and MinIO/PostgreSQL registration."""

import json
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol
from uuid import UUID

from app.common.storage.minio import MinioObjectStore, StoredObject, sha256_bytes
from app.common.storage.naming import DataZone, batch_prefix, data_object_key, manifest_object_key

MANIFEST_SCHEMA_VERSION = 1
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class BatchManifest:
    """Immutable description of one data object in one logical batch."""

    source: str
    batch_id: UUID
    logical_date: date
    object_key: str
    checksum_sha256: str
    byte_count: int
    record_count: int
    schema_version: int = MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != MANIFEST_SCHEMA_VERSION:
            raise ValueError(f"schema_version must equal {MANIFEST_SCHEMA_VERSION}")
        if not isinstance(self.source, str) or not isinstance(self.object_key, str):
            raise ValueError("source and object_key must be strings")
        if not isinstance(self.batch_id, UUID) or type(self.logical_date) is not date:
            raise ValueError("batch_id must be UUID and logical_date must be date")
        prefix = batch_prefix(self.source, self.logical_date, self.batch_id) + "/data/"
        if not self.object_key.startswith(prefix):
            raise ValueError("object_key must be inside this manifest's batch data prefix")
        filename = self.object_key.removeprefix(prefix)
        if self.object_key != data_object_key(
            self.source, self.logical_date, self.batch_id, filename
        ):
            raise ValueError("object_key must contain exactly one safe data filename")
        if not isinstance(self.checksum_sha256, str) or not _SHA256.fullmatch(self.checksum_sha256):
            raise ValueError("checksum_sha256 must be 64 lowercase hexadecimal characters")
        if (
            type(self.byte_count) is not int
            or type(self.record_count) is not int
            or self.byte_count < 0
            or self.record_count < 0
        ):
            raise ValueError("byte_count and record_count must be non-negative")

    @classmethod
    def from_stored_object(
        cls,
        *,
        source: str,
        batch_id: UUID,
        logical_date: date,
        stored: StoredObject,
        record_count: int,
    ) -> "BatchManifest":
        """Build a manifest from the verified upload result to avoid copied size/checksum errors."""
        return cls(
            source=source,
            batch_id=batch_id,
            logical_date=logical_date,
            object_key=stored.key,
            checksum_sha256=stored.checksum_sha256,
            byte_count=stored.byte_count,
            record_count=record_count,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return the stable external representation required by manifest v1."""
        return {
            "schema_version": self.schema_version,
            "source": self.source,
            "batch_id": str(self.batch_id),
            "logical_date": self.logical_date.isoformat(),
            "object_key": self.object_key,
            "checksum_sha256": self.checksum_sha256,
            "bytes": self.byte_count,
            "record_count": self.record_count,
        }

    def to_bytes(self) -> bytes:
        """Serialize deterministically for stable checksum and duplicate detection."""
        return (json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")) + "\n").encode()

    @classmethod
    def from_bytes(cls, payload: bytes) -> "BatchManifest":
        """Parse strict JSON and reject unknown or missing manifest fields."""
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("manifest must be UTF-8 JSON") from exc
        expected = {
            "schema_version",
            "source",
            "batch_id",
            "logical_date",
            "object_key",
            "checksum_sha256",
            "bytes",
            "record_count",
        }
        if not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("manifest fields do not exactly match schema version 1")
        try:
            return cls(
                schema_version=raw["schema_version"],
                source=raw["source"],
                batch_id=UUID(raw["batch_id"]),
                logical_date=date.fromisoformat(raw["logical_date"]),
                object_key=raw["object_key"],
                checksum_sha256=raw["checksum_sha256"],
                byte_count=raw["bytes"],
                record_count=raw["record_count"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid manifest v1: {exc}") from exc


class ManifestRepository(Protocol):
    """PostgreSQL operation required by the registrar."""

    def register_manifest(
        self,
        batch_id: UUID,
        *,
        source: str,
        logical_date: date,
        object_uri: str,
        record_count: int,
        content_checksum_sha256: str,
        content_bytes: int,
        manifest_uri: str,
        manifest_checksum_sha256: str,
    ) -> dict: ...


@dataclass(frozen=True, slots=True)
class RegisteredManifest:
    """The verified object reference and authoritative PostgreSQL row."""

    manifest_object: StoredObject
    database_record: dict


class ManifestRegistrar:
    """Write an immutable raw-zone manifest, then idempotently persist its reference."""

    def __init__(self, store: MinioObjectStore, repository: ManifestRepository) -> None:
        self.store = store
        self.repository = repository

    def register(
        self, manifest: BatchManifest, *, data_zone: DataZone = DataZone.RAW
    ) -> RegisteredManifest:
        """Register exact duplicates safely and reject object/database conflicts."""
        content = self.store.download(
            data_zone.bucket,
            manifest.object_key,
            expected_sha256=manifest.checksum_sha256,
        )
        if len(content) != manifest.byte_count:
            raise ValueError(
                f"manifest bytes={manifest.byte_count} but object has {len(content)} bytes"
            )
        payload = manifest.to_bytes()
        key = manifest_object_key(manifest.source, manifest.logical_date, manifest.batch_id)
        stored = self.store.upload_if_absent(
            data_zone.bucket, key, payload, content_type="application/json"
        )
        row = self.repository.register_manifest(
            manifest.batch_id,
            source=manifest.source,
            logical_date=manifest.logical_date,
            object_uri=f"s3://{data_zone.bucket}/{manifest.object_key}",
            record_count=manifest.record_count,
            content_checksum_sha256=manifest.checksum_sha256,
            content_bytes=manifest.byte_count,
            manifest_uri=stored.uri,
            manifest_checksum_sha256=sha256_bytes(payload),
        )
        return RegisteredManifest(stored, row)
