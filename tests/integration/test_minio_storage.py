"""Real MinIO operations. Set TEST_MINIO_* to opt into a local integration run."""

import os
from uuid import uuid4

import pytest
from minio import Minio

from app.common.storage.minio import MinioObjectStore, ObjectNotFound, sha256_bytes

pytestmark = pytest.mark.integration


@pytest.fixture
def minio_store():
    endpoint = os.environ.get("TEST_MINIO_ENDPOINT")
    access_key = os.environ.get("TEST_MINIO_ACCESS_KEY")
    secret_key = os.environ.get("TEST_MINIO_SECRET_KEY")
    if not all((endpoint, access_key, secret_key)):
        pytest.skip("Set TEST_MINIO_ENDPOINT, TEST_MINIO_ACCESS_KEY and TEST_MINIO_SECRET_KEY")
    secure = os.environ.get("TEST_MINIO_SECURE", "false").lower() == "true"
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
    store = MinioObjectStore(client, backoff_seconds=0.01)
    prefix = f"source=integration_{uuid4().hex}/"
    try:
        yield store, prefix
    finally:
        for item in client.list_objects("raw", prefix=prefix, recursive=True):
            client.remove_object("raw", item.object_name)


def test_real_minio_upload_download_list_checksum_and_missing(minio_store):
    store, prefix = minio_store
    key = (
        prefix
        + "logical_date=2026-11-18/"
        + "batch_id=22222222-2222-4222-8222-222222222222/data/file.bin"
    )
    content = b"integration object"
    result = store.upload("raw", key, content)
    assert result.checksum_sha256 == sha256_bytes(content)
    assert store.download("raw", key, expected_sha256=result.checksum_sha256) == content
    assert store.checksum("raw", key) == result.checksum_sha256
    assert store.exists("raw", key)
    assert store.list("raw", prefix) == (key,)
    missing = prefix + "missing.bin"
    assert not store.exists("raw", missing)
    with pytest.raises(ObjectNotFound):
        store.download("raw", missing)
