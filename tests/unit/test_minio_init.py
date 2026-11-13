"""Exercise bucket initialization without a network or Docker daemon."""

from unittest.mock import Mock

import pytest
from minio.error import S3Error

from infrastructure.minio import init_buckets


@pytest.fixture
def client(monkeypatch):
    """Replace the SDK and provide synthetic credentials."""
    for name in ("MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"):
        monkeypatch.setenv(name, "test-placeholder")
    mock = Mock()
    monkeypatch.setattr(init_buckets, "Minio", Mock(return_value=mock))
    return mock


def test_creates_all_four_buckets(client):
    client.bucket_exists.return_value = False
    init_buckets.main()
    assert [call.args[0] for call in client.make_bucket.call_args_list] == [
        "raw",
        "staging",
        "curated",
        "rejected",
    ]


def test_repeated_initialization_preserves_existing_buckets(client):
    existing = set()
    client.bucket_exists.side_effect = lambda bucket: bucket in existing
    client.make_bucket.side_effect = existing.add
    init_buckets.main()
    init_buckets.main()
    assert client.make_bucket.call_count == 4
    assert existing == set(init_buckets.BUCKETS)


def test_accepts_concurrent_creation_by_same_owner(client):
    client.bucket_exists.return_value = False
    client.make_bucket.side_effect = S3Error(
        response=None,
        code="BucketAlreadyOwnedByYou",
        message="already created",
        resource="",
        request_id="",
        host_id="",
    )
    init_buckets.main()
    assert client.make_bucket.call_count == 4


def test_propagates_permission_errors(client):
    client.bucket_exists.return_value = False
    client.make_bucket.side_effect = S3Error(
        response=None, code="AccessDenied", message="denied", resource="", request_id="", host_id=""
    )
    with pytest.raises(S3Error, match="AccessDenied"):
        init_buckets.main()
