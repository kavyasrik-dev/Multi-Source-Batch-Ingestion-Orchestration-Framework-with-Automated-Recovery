"""HTTP/JSON adapter tests against a deterministic local fixture server."""

import hashlib
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from app.common.storage.minio import StoredObject
from app.connectors import SourceAdapter
from app.connectors.http_json import (
    HTTPJSONAdapter,
    HTTPJSONAdapterError,
    HTTPJSONConfig,
    HTTPResponseError,
    HTTPRetryExhausted,
    JSONPayloadError,
    JSONShapeError,
)


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def upload_if_absent(
        self, bucket: str, key: str, data: bytes, *, content_type: str
    ) -> StoredObject:
        assert content_type == "application/json"
        existing = self.objects.setdefault((bucket, key), data)
        assert existing == data
        digest = hashlib.sha256(data).hexdigest()
        return StoredObject(bucket, key, digest, len(data), None)


class FixtureHandler(BaseHTTPRequestHandler):
    retry_count = 0

    def do_GET(self) -> None:
        parsed = urlsplit(self.path)
        query = parse_qs(parsed.query)
        if parsed.path == "/pages":
            cursor = query.get("cursor", [None])[0]
            if cursor is None:
                self._json(200, {"items": [{"id": 1}, {"id": 2}], "next_cursor": "two"})
            elif cursor == "two":
                self._json(200, {"items": [{"id": 3}], "next_cursor": None})
            else:
                self._json(400, {"error": "bad cursor"})
        elif parsed.path == "/empty":
            self._json(200, {"items": [], "next_cursor": None})
        elif parsed.path == "/malformed":
            self._send(200, b'{"items": [')
        elif parsed.path == "/wrong-shape":
            self._json(200, {"items": {"id": 1}, "next_cursor": None})
        elif parsed.path == "/wrong-fields":
            self._json(200, {"items": [{"other": 1}], "next_cursor": None})
        elif parsed.path == "/repeat-cursor":
            self._json(200, {"items": [], "next_cursor": "same"})
        elif parsed.path == "/retry":
            type(self).retry_count += 1
            if type(self).retry_count == 1:
                self._json(429, {"error": "rate limited"})
            elif type(self).retry_count == 2:
                self._json(503, {"error": "unavailable"})
            else:
                self._json(200, {"items": [{"id": 9}], "next_cursor": None})
        elif parsed.path == "/always-500":
            self._json(500, {"error": "failed"})
        elif parsed.path == "/bad-request":
            self._json(400, {"error": "invalid"})
        else:
            self._json(404, {"error": "missing"})

    def _json(self, status: int, document: object) -> None:
        self._send(status, json.dumps(document, separators=(",", ":")).encode())

    def _send(self, status: int, payload: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        pass


@contextmanager
def fixture_server() -> Iterator[str]:
    FixtureHandler.retry_count = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


@pytest.fixture
def endpoint() -> Iterator[str]:
    with fixture_server() as url:
        yield url


def build(endpoint: str, path: str = "/pages", **changes):
    values = {
        "source_name": "orders-api",
        "endpoint": endpoint + path,
        "expected_fields": ("id",),
        "backoff_seconds": 0,
    }
    values.update(changes)
    store = MemoryStore()
    adapter = HTTPJSONAdapter(HTTPJSONConfig(**values), store, sleep=lambda _: None)
    return adapter, store


def test_pagination_raw_persistence_bounded_batches_and_stable_ids(endpoint):
    adapter, store = build(endpoint)
    assert isinstance(adapter, SourceAdapter)
    [source] = adapter.discover()
    first = list(adapter.iter_batches(source, max_records=2))
    replay = list(adapter.iter_batches(source, max_records=2))
    assert [len(batch.records) for batch in first] == [2, 1]
    assert [record.values["id"] for batch in first for record in batch.records] == [1, 2, 3]
    assert [batch.batch_id for batch in first] == [batch.batch_id for batch in replay]
    assert len(store.objects) == 2
    assert all(bucket == "raw" for bucket, _ in store.objects)
    assert len(adapter.request_metadata) == 2
    assert adapter.metadata(source).byte_count == sum(map(len, store.objects.values()))


def test_empty_response_produces_no_batches_but_persists_payload(endpoint):
    adapter, store = build(endpoint, "/empty")
    [source] = adapter.discover()
    assert list(adapter.iter_batches(source, max_records=10)) == []
    assert len(store.objects) == 1


def test_malformed_json_is_persisted_before_actionable_failure(endpoint):
    adapter, store = build(endpoint, "/malformed")
    [source] = adapter.discover()
    with pytest.raises(JSONPayloadError, match="page 1.*UTF-8 JSON"):
        list(adapter.extract(source))
    assert list(store.objects.values()) == [b'{"items": [']


@pytest.mark.parametrize(
    ("path", "message"),
    [
        ("/wrong-shape", "records value must be an array"),
        ("/wrong-fields", "expected fields"),
        ("/repeat-cursor", "repeated next cursor"),
    ],
)
def test_response_shape_validation(endpoint, path, message):
    adapter, store = build(endpoint, path, max_pages=2)
    [source] = adapter.discover()
    with pytest.raises(JSONShapeError, match=message):
        list(adapter.extract(source))
    assert store.objects


def test_429_and_5xx_are_retried_with_bounded_backoff_and_all_payloads_saved(endpoint):
    sleeps = []
    store = MemoryStore()
    config = HTTPJSONConfig(
        source_name="orders-api",
        endpoint=endpoint + "/retry",
        expected_fields=("id",),
        max_attempts=3,
        backoff_seconds=0.5,
    )
    adapter = HTTPJSONAdapter(config, store, sleep=sleeps.append)
    [source] = adapter.discover()
    [record] = list(adapter.extract(source))
    assert record.values["id"] == 9
    assert sleeps == [0.5, 1.0]
    assert [item.status_code for item in adapter.request_metadata] == [429, 503, 200]
    assert len(store.objects) == 3


def test_5xx_retry_limit_is_enforced(endpoint):
    adapter, store = build(endpoint, "/always-500", max_attempts=2)
    [source] = adapter.discover()
    with pytest.raises(HTTPRetryExhausted, match="after 2 attempts: HTTP 500"):
        list(adapter.extract(source))
    assert len(adapter.request_metadata) == 2
    assert len(store.objects) == 2


def test_non_transient_http_error_is_not_retried_but_is_persisted(endpoint):
    adapter, store = build(endpoint, "/bad-request")
    [source] = adapter.discover()
    with pytest.raises(HTTPResponseError, match="HTTP 400"):
        list(adapter.extract(source))
    assert len(adapter.request_metadata) == 1
    assert len(store.objects) == 1


class TimeoutSession:
    def __init__(self) -> None:
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        raise requests.Timeout("deterministic timeout")


def test_timeouts_are_bounded_and_audited_without_payload():
    store = MemoryStore()
    session = TimeoutSession()
    config = HTTPJSONConfig(
        source_name="orders-api",
        endpoint="http://127.0.0.1:9/orders",
        timeout_seconds=0.01,
        max_attempts=2,
        backoff_seconds=0,
    )
    adapter = HTTPJSONAdapter(config, store, session=session, sleep=lambda _: None)
    [source] = adapter.discover()
    with pytest.raises(HTTPRetryExhausted, match="after 2 attempts: Timeout"):
        list(adapter.extract(source))
    assert len(session.calls) == 2
    assert all(call[1]["timeout"] == 0.01 for call in session.calls)
    assert [item.error_type for item in adapter.request_metadata] == ["Timeout", "Timeout"]
    assert not store.objects


def test_request_metadata_excludes_secrets_and_values(endpoint):
    adapter, _ = build(
        endpoint,
        "/empty",
        parameters={"tenant": "customer-secret"},
        headers={
            "Authorization": "Bearer top-secret",
            "X-API-Key": "api-secret",
            "X-Correlation-ID": "trace-123",
        },
    )
    [source] = adapter.discover()
    list(adapter.extract(source))
    [audit] = adapter.request_metadata
    rendered = repr(audit)
    assert audit.header_names == ("X-Correlation-ID",)
    assert audit.parameter_names == ("tenant",)
    assert "top-secret" not in rendered
    assert "api-secret" not in rendered
    assert "customer-secret" not in rendered
    assert "?" not in audit.url


def test_source_validation_and_configuration_errors(endpoint):
    adapter, _ = build(endpoint)
    with pytest.raises(HTTPJSONAdapterError, match="not discovered"):
        list(adapter.extract(type(adapter.discover()[0])("http://other", "other")))
    with pytest.raises(ValueError, match="credentials, query, or fragment"):
        HTTPJSONConfig(source_name="orders", endpoint=endpoint + "/x?token=secret")
    with pytest.raises(ValueError, match="safe lowercase identifier"):
        HTTPJSONConfig(source_name="Orders API", endpoint=endpoint)


@pytest.mark.parametrize("max_records", [0, -1, True, 1.5])
def test_invalid_batch_bounds_are_rejected_without_network(endpoint, max_records):
    adapter, _ = build(endpoint)
    [source] = adapter.discover()
    with pytest.raises(ValueError, match="positive integer"):
        list(adapter.iter_batches(source, max_records=max_records))
