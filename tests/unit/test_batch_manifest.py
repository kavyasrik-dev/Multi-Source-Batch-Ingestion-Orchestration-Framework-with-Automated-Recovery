"""Manifest schema, deterministic naming, and duplicate registration tests."""

from datetime import date
from uuid import UUID

import pytest

from app.common.storage.manifest import BatchManifest, ManifestRegistrar
from app.common.storage.minio import (
    MinioObjectStore,
    ObjectConflict,
    ObjectNotFound,
    sha256_bytes,
)
from app.common.storage.naming import (
    DataZone,
    batch_prefix,
    data_object_key,
    manifest_object_key,
)
from tests.unit.test_minio_storage import FakeMinio

BATCH_ID = UUID("22222222-2222-4222-8222-222222222222")
LOGICAL_DATE = date(2026, 11, 18)


class FakeManifestRepository:
    def __init__(self):
        self.registration = None
        self.calls = 0

    def register_manifest(self, batch_id, **values):
        candidate = {"id": batch_id, **values}
        self.calls += 1
        if self.registration is not None and self.registration != candidate:
            raise ValueError("manifest conflict")
        self.registration = candidate
        return candidate


def manifest(**changes):
    content = b"raw batch bytes"
    values = {
        "source": "sales_api",
        "batch_id": BATCH_ID,
        "logical_date": LOGICAL_DATE,
        "object_key": data_object_key("sales_api", LOGICAL_DATE, BATCH_ID, "part-00000.parquet"),
        "checksum_sha256": sha256_bytes(content),
        "byte_count": len(content),
        "record_count": 12,
    }
    values.update(changes)
    return BatchManifest(**values)


def test_zone_prefixes_and_names_are_consistent():
    prefix = batch_prefix("sales_api", LOGICAL_DATE, BATCH_ID)
    assert prefix == f"source=sales_api/logical_date=2026-11-18/batch_id={BATCH_ID}"
    assert manifest_object_key("sales_api", LOGICAL_DATE, BATCH_ID) == f"{prefix}/manifest.v1.json"
    assert set(DataZone) == {DataZone.RAW, DataZone.STAGING, DataZone.CURATED, DataZone.REJECTED}
    assert {zone.bucket for zone in DataZone} == {"raw", "staging", "curated", "rejected"}


def test_manifest_round_trip_is_deterministic_and_complete():
    item = manifest()
    assert BatchManifest.from_bytes(item.to_bytes()) == item
    assert item.to_bytes().endswith(b"\n")
    assert item.to_dict()["bytes"] == len(b"raw batch bytes")
    assert set(item.to_dict()) == {
        "schema_version",
        "source",
        "batch_id",
        "logical_date",
        "object_key",
        "checksum_sha256",
        "bytes",
        "record_count",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"checksum_sha256": "bad"},
        {"byte_count": -1},
        {"record_count": -1},
        {"schema_version": 2},
    ],
)
def test_invalid_manifest_is_rejected(changes):
    with pytest.raises(ValueError):
        manifest(**changes)


def test_manifest_registration_is_idempotent_and_persists_reference():
    sdk = FakeMinio()
    store = MinioObjectStore(sdk, backoff_seconds=0, sleep=lambda _: None)
    repository = FakeManifestRepository()
    registrar = ManifestRegistrar(store, repository)
    item = manifest()
    store.upload(DataZone.RAW.bucket, item.object_key, b"raw batch bytes")
    first = registrar.register(item)
    second = registrar.register(item)
    assert first.database_record == second.database_record
    assert repository.calls == 2
    assert sdk.put_calls == 2  # One data object and one manifest; retry reuses both.
    assert first.manifest_object.uri.endswith("/manifest.v1.json")
    assert first.database_record["manifest_uri"] == first.manifest_object.uri
    assert first.database_record["object_uri"] == f"s3://raw/{item.object_key}"
    assert first.database_record["manifest_checksum_sha256"] == sha256_bytes(item.to_bytes())


def test_duplicate_manifest_with_different_content_is_rejected_before_database_write():
    sdk = FakeMinio()
    store = MinioObjectStore(sdk, backoff_seconds=0, sleep=lambda _: None)
    repository = FakeManifestRepository()
    registrar = ManifestRegistrar(store, repository)
    item = manifest()
    store.upload(DataZone.RAW.bucket, item.object_key, b"raw batch bytes")
    registrar.register(item)
    with pytest.raises(ObjectConflict, match="different bytes"):
        registrar.register(manifest(record_count=13))
    assert repository.calls == 1


def test_manifest_parser_rejects_unknown_fields():
    payload = manifest().to_bytes().rstrip()[:-1] + b',"extra":true}\n'
    with pytest.raises(ValueError, match="fields"):
        BatchManifest.from_bytes(payload)


def test_manifest_is_derived_from_upload_and_verifies_object_before_registration():
    sdk = FakeMinio()
    store = MinioObjectStore(sdk, backoff_seconds=0, sleep=lambda _: None)
    repository = FakeManifestRepository()
    key = data_object_key("sales_api", LOGICAL_DATE, BATCH_ID, "part-00000.parquet")
    uploaded = store.upload(DataZone.RAW.bucket, key, b"raw batch bytes")
    item = BatchManifest.from_stored_object(
        source="sales_api",
        batch_id=BATCH_ID,
        logical_date=LOGICAL_DATE,
        stored=uploaded,
        record_count=12,
    )
    assert item.checksum_sha256 == uploaded.checksum_sha256
    registrar = ManifestRegistrar(store, repository)
    with pytest.raises(ValueError, match="object has"):
        registrar.register(manifest(byte_count=999))
    assert repository.calls == 0


def test_manifest_registration_requires_existing_content_object():
    store = MinioObjectStore(FakeMinio(), backoff_seconds=0, sleep=lambda _: None)
    repository = FakeManifestRepository()
    with pytest.raises(ObjectNotFound, match="s3://raw/"):
        ManifestRegistrar(store, repository).register(manifest())
    assert repository.calls == 0
