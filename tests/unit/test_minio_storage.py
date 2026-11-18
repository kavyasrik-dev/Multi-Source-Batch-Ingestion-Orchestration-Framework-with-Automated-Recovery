"""MinIO wrapper tests using an in-memory SDK double with realistic failures."""

from io import BytesIO
from types import SimpleNamespace

import pytest
from minio.error import S3Error

from app.common.storage.minio import (
    ChecksumMismatch,
    MinioObjectStore,
    ObjectNotFound,
    TransientObjectStoreError,
    sha256_bytes,
)


def missing_error():
    return S3Error(None, "NoSuchKey", "missing", "", "", "")


class Response(BytesIO):
    def __init__(self, data):
        super().__init__(data)
        self.released = False

    def release_conn(self):
        self.released = True


class FakeMinio:
    def __init__(self):
        self.objects = {}
        self.metadata = {}
        self.put_calls = 0
        self.fail_puts = 0
        self.last_response = None

    def put_object(self, bucket, key, stream, length, *, content_type, metadata):
        self.put_calls += 1
        if self.fail_puts:
            self.fail_puts -= 1
            raise ConnectionError("temporary network failure")
        data = stream.read()
        assert len(data) == length
        self.objects[(bucket, key)] = data
        self.metadata[(bucket, key)] = {"content-type": content_type, **metadata}
        return SimpleNamespace(etag="etag-1")

    def get_object(self, bucket, key):
        try:
            data = self.objects[(bucket, key)]
        except KeyError as exc:
            raise missing_error() from exc
        self.last_response = Response(data)
        return self.last_response

    def stat_object(self, bucket, key):
        if (bucket, key) not in self.objects:
            raise missing_error()
        return SimpleNamespace(size=len(self.objects[(bucket, key)]))

    def list_objects(self, bucket, *, prefix, recursive):
        assert recursive is True
        return [
            SimpleNamespace(object_name=key)
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        ]


@pytest.fixture
def storage():
    sdk = FakeMinio()
    return sdk, MinioObjectStore(sdk, max_attempts=3, backoff_seconds=0, sleep=lambda _: None)


def test_upload_download_list_exists_and_checksum(storage):
    sdk, store = storage
    content = b"canonical parquet bytes"
    uploaded = store.upload("raw", "source=a/data/part.parquet", content)
    assert uploaded.checksum_sha256 == sha256_bytes(content)
    assert uploaded.byte_count == len(content)
    assert uploaded.uri == "s3://raw/source=a/data/part.parquet"
    assert sdk.metadata[("raw", uploaded.key)]["sha256"] == uploaded.checksum_sha256
    assert store.exists("raw", uploaded.key)
    assert store.download("raw", uploaded.key, expected_sha256=uploaded.checksum_sha256) == content
    assert sdk.last_response.closed and sdk.last_response.released
    assert store.checksum("raw", uploaded.key) == uploaded.checksum_sha256
    store.upload("raw", "source=a/data/another.parquet", b"2")
    assert store.list("raw", "source=a/") == (
        "source=a/data/another.parquet",
        "source=a/data/part.parquet",
    )


def test_missing_object_has_clear_semantics(storage):
    _, store = storage
    assert not store.exists("raw", "missing/file")
    with pytest.raises(ObjectNotFound, match="s3://raw/missing/file"):
        store.download("raw", "missing/file")


def test_checksum_mismatch_is_detected(storage):
    _, store = storage
    store.upload("raw", "source=a/data/file", b"actual")
    with pytest.raises(ChecksumMismatch, match="downloaded"):
        store.download("raw", "source=a/data/file", expected_sha256="0" * 64)


def test_transient_failure_is_retried_from_rewindable_bytes(storage):
    sdk, store = storage
    sdk.fail_puts = 2
    result = store.upload("raw", "source=a/data/file", b"content")
    assert result.byte_count == 7
    assert sdk.put_calls == 3


def test_transient_failure_exhaustion_is_normalized():
    sdk = FakeMinio()
    sdk.fail_puts = 3
    delays = []
    store = MinioObjectStore(sdk, max_attempts=2, backoff_seconds=0.5, sleep=delays.append)
    with pytest.raises(TransientObjectStoreError, match="2 attempts"):
        store.upload("raw", "source=a/data/file", b"content")
    assert delays == [0.5]


@pytest.mark.parametrize("key", ["/absolute", "a//b", "a/../b", "a\\b"])
def test_unsafe_object_keys_are_rejected(storage, key):
    _, store = storage
    with pytest.raises(ValueError):
        store.upload("raw", key, b"content")
