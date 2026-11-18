"""Retrying, checksum-aware MinIO operations behind a small reusable interface."""

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from io import BytesIO
from typing import Any, TypeVar

from minio import Minio
from minio.error import S3Error
from urllib3.exceptions import HTTPError

T = TypeVar("T")
_MISSING_CODES = frozenset({"NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound"})
_TRANSIENT_CODES = frozenset(
    {"InternalError", "RequestTimeout", "ServiceUnavailable", "SlowDown", "Throttling"}
)


class ObjectStoreError(RuntimeError):
    """Base error for normalized object-store failures."""


class ObjectNotFound(ObjectStoreError):
    """The requested bucket or object does not exist."""


class TransientObjectStoreError(ObjectStoreError):
    """A retryable failure exhausted the configured attempt limit."""


class ChecksumMismatch(ObjectStoreError):
    """Downloaded or existing bytes do not match the expected SHA-256."""


class ObjectConflict(ObjectStoreError):
    """An immutable deterministic key already contains different bytes."""


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Stable information returned after a verified upload."""

    bucket: str
    key: str
    checksum_sha256: str
    byte_count: int
    etag: str | None

    @property
    def uri(self) -> str:
        """Return an S3-compatible URI."""
        return f"s3://{self.bucket}/{self.key}"


def sha256_bytes(data: bytes) -> str:
    """Return lowercase SHA-256 hex for immutable object content."""
    return hashlib.sha256(data).hexdigest()


def _validate_location(bucket: str, key: str) -> None:
    if not bucket or not key or key.startswith("/") or "\\" in key:
        raise ValueError("bucket and relative forward-slash object key are required")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise ValueError("object key contains an unsafe path segment")


class MinioObjectStore:
    """Byte-oriented MinIO client with bounded retries and verified immutable writes."""

    def __init__(
        self,
        client: Minio,
        *,
        max_attempts: int = 3,
        backoff_seconds: float = 0.25,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if max_attempts < 1 or backoff_seconds < 0:
            raise ValueError("max_attempts must be positive and backoff_seconds non-negative")
        self.client = client
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.sleep = sleep

    @classmethod
    def connect(
        cls, endpoint: str, access_key: str, secret_key: str, *, secure: bool = True, **kwargs: Any
    ) -> "MinioObjectStore":
        """Construct from explicit credentials supplied by environment-aware callers."""
        return cls(
            Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure),
            **kwargs,
        )

    def _call(self, operation: Callable[[], T], *, bucket: str, key: str = "") -> T:
        last_error: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation()
            except S3Error as exc:
                if exc.code in _MISSING_CODES:
                    raise ObjectNotFound(f"s3://{bucket}/{key}") from exc
                if exc.code not in _TRANSIENT_CODES:
                    raise ObjectStoreError(
                        f"MinIO rejected s3://{bucket}/{key}: {exc.code}"
                    ) from exc
                last_error = exc
            except (HTTPError, ConnectionError, TimeoutError, OSError) as exc:
                last_error = exc
            if attempt < self.max_attempts:
                self.sleep(self.backoff_seconds * (2 ** (attempt - 1)))
        raise TransientObjectStoreError(
            f"MinIO operation failed after {self.max_attempts} attempts: s3://{bucket}/{key}"
        ) from last_error

    def upload(
        self, bucket: str, key: str, data: bytes, *, content_type: str = "application/octet-stream"
    ) -> StoredObject:
        """Upload bytes with a SHA-256 user-metadata value and bounded retry."""
        _validate_location(bucket, key)
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        digest = sha256_bytes(data)

        def put():
            return self.client.put_object(
                bucket,
                key,
                BytesIO(data),
                len(data),
                content_type=content_type,
                metadata={"sha256": digest},
            )

        result = self._call(put, bucket=bucket, key=key)
        return StoredObject(bucket, key, digest, len(data), getattr(result, "etag", None))

    def download(self, bucket: str, key: str, *, expected_sha256: str | None = None) -> bytes:
        """Download all bytes, always release the HTTP response, and optionally verify SHA-256."""
        _validate_location(bucket, key)

        def get() -> bytes:
            response = self.client.get_object(bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        data = self._call(get, bucket=bucket, key=key)
        actual = sha256_bytes(data)
        if expected_sha256 is not None and actual != expected_sha256:
            raise ChecksumMismatch(
                f"s3://{bucket}/{key}: expected {expected_sha256}, downloaded {actual}"
            )
        return data

    def checksum(self, bucket: str, key: str) -> str:
        """Compute SHA-256 from downloaded bytes instead of trusting object metadata or ETag."""
        return sha256_bytes(self.download(bucket, key))

    def exists(self, bucket: str, key: str) -> bool:
        """Return false only for missing resources; propagate permissions and exhausted retries."""
        _validate_location(bucket, key)
        try:
            self._call(lambda: self.client.stat_object(bucket, key), bucket=bucket, key=key)
        except ObjectNotFound:
            return False
        return True

    def list(self, bucket: str, prefix: str) -> tuple[str, ...]:
        """List recursive object keys in deterministic lexical order."""
        if not bucket or prefix.startswith("/") or "\\" in prefix:
            raise ValueError("bucket and a relative forward-slash prefix are required")

        def collect() -> tuple[str, ...]:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            return tuple(sorted(item.object_name for item in objects if item.object_name))

        return self._call(collect, bucket=bucket, key=prefix)

    def upload_if_absent(
        self, bucket: str, key: str, data: bytes, *, content_type: str
    ) -> StoredObject:
        """Reuse identical immutable content and reject a deterministic-key collision."""
        digest = sha256_bytes(data)
        if self.exists(bucket, key):
            try:
                existing = self.download(bucket, key, expected_sha256=digest)
            except ChecksumMismatch as exc:
                raise ObjectConflict(
                    f"s3://{bucket}/{key} already contains different bytes"
                ) from exc
            return StoredObject(bucket, key, digest, len(existing), None)
        uploaded = self.upload(bucket, key, data, content_type=content_type)
        try:
            self.download(bucket, key, expected_sha256=digest)
        except ChecksumMismatch as exc:
            raise ObjectConflict(f"s3://{bucket}/{key} changed during registration") from exc
        return uploaded
