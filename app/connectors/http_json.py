"""Retrying HTTP/JSON adapter with raw-response persistence before normalization."""

import hashlib
import json
import re
import time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit
from uuid import NAMESPACE_URL, uuid5

import requests

from app.common.storage.minio import StoredObject
from app.connectors.base import ExtractedBatch, SourceMetadata, SourceObject, SourceRecord

_SENSITIVE_HEADERS = frozenset(
    {"authorization", "cookie", "proxy-authorization", "x-api-key", "x-auth-token"}
)
_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


class HTTPJSONAdapterError(RuntimeError):
    """Base error for HTTP transport, status, JSON, or response-shape failures."""


class HTTPRetryExhausted(HTTPJSONAdapterError):
    """A transient response or transport failure exhausted bounded retries."""


class HTTPResponseError(HTTPJSONAdapterError):
    """The endpoint returned a non-retryable HTTP response."""


class JSONPayloadError(HTTPJSONAdapterError):
    """A response is not valid UTF-8 JSON."""


class JSONShapeError(HTTPJSONAdapterError):
    """A JSON response does not match the configured pagination/record contract."""


class RawResponseStore(Protocol):
    """Immutable byte-store operation required by the adapter."""

    def upload_if_absent(
        self, bucket: str, key: str, data: bytes, *, content_type: str
    ) -> StoredObject: ...


@dataclass(frozen=True, slots=True)
class HTTPJSONConfig:
    """Endpoint, pagination, validation, timeout, and bounded retry policy."""

    source_name: str
    endpoint: str
    records_path: tuple[str, ...] = ("items",)
    next_cursor_path: tuple[str, ...] = ("next_cursor",)
    cursor_parameter: str = "cursor"
    parameters: Mapping[str, str] | None = None
    headers: Mapping[str, str] | None = None
    expected_fields: tuple[str, ...] | None = None
    timeout_seconds: float = 10.0
    max_attempts: int = 3
    backoff_seconds: float = 0.25
    max_pages: int = 1000
    raw_bucket: str = "raw"

    def __post_init__(self) -> None:
        parsed = urlsplit(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("endpoint must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("endpoint cannot contain credentials, query, or fragment")
        if not _SAFE_NAME.fullmatch(self.source_name):
            raise ValueError("source_name must be a safe lowercase identifier")
        if not self.records_path or not all(self.records_path):
            raise ValueError("records_path must contain at least one non-empty segment")
        if not self.next_cursor_path or not all(self.next_cursor_path):
            raise ValueError("next_cursor_path must contain at least one non-empty segment")
        if not self.cursor_parameter:
            raise ValueError("cursor_parameter cannot be empty")
        if self.expected_fields is not None and (
            not self.expected_fields or len(set(self.expected_fields)) != len(self.expected_fields)
        ):
            raise ValueError("expected_fields must be non-empty and unique")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.max_attempts < 1 or self.backoff_seconds < 0:
            raise ValueError("max_attempts must be positive and backoff_seconds non-negative")
        if self.max_pages < 1:
            raise ValueError("max_pages must be positive")
        if not self.raw_bucket:
            raise ValueError("raw_bucket cannot be empty")


@dataclass(frozen=True, slots=True)
class RequestMetadata:
    """Secret-free request audit metadata retained in memory for orchestration logging."""

    method: str
    url: str
    page_number: int
    attempt: int
    status_code: int | None
    elapsed_seconds: float
    parameter_names: tuple[str, ...]
    header_names: tuple[str, ...]
    raw_object_uri: str | None
    error_type: str | None


@dataclass(frozen=True, slots=True)
class _Snapshot:
    metadata: SourceMetadata
    records: tuple[SourceRecord, ...]


def _at_path(value: object, path: tuple[str, ...], *, label: str) -> object:
    current = value
    for segment in path:
        if not isinstance(current, dict) or segment not in current:
            dotted = ".".join(path)
            raise JSONShapeError(f"response is missing {label} path '{dotted}'")
        current = current[segment]
    return current


class HTTPJSONAdapter:
    """Fetch a paginated JSON snapshot, persisting every response before parsing it."""

    def __init__(
        self,
        config: HTTPJSONConfig,
        store: RawResponseStore,
        *,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.store = store
        self.session = session or requests.Session()
        self.sleep = sleep
        self.clock = clock
        self._snapshots: dict[str, _Snapshot] = {}
        self._request_metadata: list[RequestMetadata] = []

    @property
    def request_metadata(self) -> tuple[RequestMetadata, ...]:
        """Return immutable, secret-free audit records for all attempted requests."""
        return tuple(self._request_metadata)

    def discover(self) -> tuple[SourceObject, ...]:
        """Expose the configured endpoint without making a network request."""
        return (SourceObject(locator=self.config.endpoint, name=self.config.source_name),)

    def _validate_source(self, source: SourceObject) -> None:
        if source != self.discover()[0]:
            raise HTTPJSONAdapterError("source was not discovered by this HTTP adapter")

    def metadata(self, source: SourceObject) -> SourceMetadata:
        """Fetch once per adapter instance and return checksum-backed snapshot metadata."""
        return self._snapshot(source).metadata

    def extract(self, source: SourceObject) -> Iterator[SourceRecord]:
        """Iterate normalized records from the persisted raw-response snapshot."""
        return iter(self._snapshot(source).records)

    def iter_batches(self, source: SourceObject, *, max_records: int) -> Iterator[ExtractedBatch]:
        """Yield bounded batches with IDs stable for identical payload and configuration."""
        if isinstance(max_records, bool) or not isinstance(max_records, int) or max_records <= 0:
            raise ValueError("max_records must be a positive integer")
        snapshot = self._snapshot(source)
        schema = "\x1f".join(self.config.expected_fields or ())
        for start in range(0, len(snapshot.records), max_records):
            records = snapshot.records[start : start + max_records]
            end = start + len(records)
            identity = f"http-json-batch\0{snapshot.metadata.identity}\0{schema}\0{start}\0{end}"
            yield ExtractedBatch(
                batch_id=uuid5(NAMESPACE_URL, identity),
                source=source,
                source_metadata=snapshot.metadata,
                start_record=start,
                records=records,
            )

    def _snapshot(self, source: SourceObject) -> _Snapshot:
        self._validate_source(source)
        cached = self._snapshots.get(source.locator)
        if cached is not None:
            return cached
        snapshot = self._fetch_snapshot(source)
        self._snapshots[source.locator] = snapshot
        return snapshot

    def _fetch_snapshot(self, source: SourceObject) -> _Snapshot:
        records: list[SourceRecord] = []
        payload_digests: list[str] = []
        total_bytes = 0
        cursor: str | None = None
        seen_cursors: set[str] = set()
        fetched_at = datetime.now(UTC)
        for page_number in range(1, self.config.max_pages + 1):
            payload = self._request_page(page_number, cursor)
            total_bytes += len(payload)
            payload_digests.append(hashlib.sha256(payload).hexdigest())
            document = self._decode(payload, page_number)
            items = _at_path(document, self.config.records_path, label="records")
            if not isinstance(items, list):
                raise JSONShapeError(f"page {page_number}: records value must be an array")
            for index, item in enumerate(items, start=1):
                if not isinstance(item, dict) or not all(isinstance(key, str) for key in item):
                    raise JSONShapeError(
                        f"page {page_number} record {index}: each record must be a JSON object"
                    )
                if self.config.expected_fields is not None and set(item) != set(
                    self.config.expected_fields
                ):
                    raise JSONShapeError(
                        f"page {page_number} record {index}: expected fields "
                        f"{self.config.expected_fields!r}, received {tuple(item)!r}"
                    )
                records.append(
                    SourceRecord(values=MappingProxyType(dict(item)), line_number=len(records) + 1)
                )
            next_cursor = _at_path(document, self.config.next_cursor_path, label="cursor")
            if next_cursor is None:
                break
            if not isinstance(next_cursor, (str, int)) or isinstance(next_cursor, bool):
                raise JSONShapeError(
                    f"page {page_number}: next cursor must be string, integer, or null"
                )
            cursor = str(next_cursor)
            if not cursor or cursor in seen_cursors:
                raise JSONShapeError(f"page {page_number}: empty or repeated next cursor")
            seen_cursors.add(cursor)
        else:
            raise JSONShapeError(f"pagination exceeded max_pages={self.config.max_pages}")
        aggregate = hashlib.sha256("\0".join(payload_digests).encode()).hexdigest()
        identity = uuid5(
            NAMESPACE_URL,
            f"http-json\0{source.locator}\0{aggregate}\0{len(records)}",
        )
        return _Snapshot(
            metadata=SourceMetadata(
                identity=identity,
                checksum_sha256=aggregate,
                byte_count=total_bytes,
                modified_at=fetched_at,
                media_type="application/json",
            ),
            records=tuple(records),
        )

    def _request_page(self, page_number: int, cursor: str | None) -> bytes:
        parameters = dict(self.config.parameters or {})
        if cursor is not None:
            parameters[self.config.cursor_parameter] = cursor
        headers = dict(self.config.headers or {})
        last_reason = "transient HTTP failure"
        for attempt in range(1, self.config.max_attempts + 1):
            started = self.clock()
            try:
                response = self.session.get(
                    self.config.endpoint,
                    params=parameters,
                    headers=headers,
                    timeout=self.config.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                elapsed = self.clock() - started
                last_reason = type(exc).__name__
                self._record_request(
                    page_number, attempt, None, elapsed, parameters, headers, None, last_reason
                )
            else:
                elapsed = self.clock() - started
                payload = response.content
                stored = self._persist_response(page_number, attempt, response.status_code, payload)
                self._record_request(
                    page_number,
                    attempt,
                    response.status_code,
                    elapsed,
                    parameters,
                    headers,
                    stored.uri,
                    None,
                )
                if 200 <= response.status_code < 300:
                    return payload
                if response.status_code != 429 and not 500 <= response.status_code < 600:
                    raise HTTPResponseError(
                        f"GET {self.config.endpoint} returned HTTP {response.status_code}"
                    )
                last_reason = f"HTTP {response.status_code}"
            if attempt < self.config.max_attempts:
                self.sleep(self.config.backoff_seconds * (2 ** (attempt - 1)))
        raise HTTPRetryExhausted(
            f"GET {self.config.endpoint} failed after {self.config.max_attempts} attempts: "
            f"{last_reason}"
        )

    def _persist_response(
        self, page_number: int, attempt: int, status_code: int, payload: bytes
    ) -> StoredObject:
        digest = hashlib.sha256(payload).hexdigest()
        key = (
            f"http/{self.config.source_name}/responses/"
            f"page-{page_number:06d}/attempt-{attempt:02d}-{status_code}-{digest}.json"
        )
        return self.store.upload_if_absent(
            self.config.raw_bucket, key, payload, content_type="application/json"
        )

    def _record_request(
        self,
        page_number: int,
        attempt: int,
        status_code: int | None,
        elapsed: float,
        parameters: Mapping[str, str],
        headers: Mapping[str, str],
        raw_uri: str | None,
        error_type: str | None,
    ) -> None:
        parsed = urlsplit(self.config.endpoint)
        safe_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
        safe_headers = tuple(
            sorted(name for name in headers if name.casefold() not in _SENSITIVE_HEADERS)
        )
        self._request_metadata.append(
            RequestMetadata(
                method="GET",
                url=safe_url,
                page_number=page_number,
                attempt=attempt,
                status_code=status_code,
                elapsed_seconds=max(0.0, elapsed),
                parameter_names=tuple(sorted(parameters)),
                header_names=safe_headers,
                raw_object_uri=raw_uri,
                error_type=error_type,
            )
        )

    @staticmethod
    def _decode(payload: bytes, page_number: int) -> object:
        try:
            return json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise JSONPayloadError(f"page {page_number}: response is not valid UTF-8 JSON") from exc
