# HTTP/JSON source adapter (Day 8)

`HTTPJSONAdapter` implements the common source contract for one configured HTTP(S)
endpoint. Discovery is side-effect free. The first metadata, extraction, or batch call
fetches a complete paginated snapshot; later calls on the same adapter instance replay
that immutable in-memory snapshot rather than contacting the endpoint again.

## Configuration and pagination

`HTTPJSONConfig` defines the source name, endpoint, records path, next-cursor path,
cursor query parameter, static parameters/headers, exact expected record fields,
request timeout, retry policy, page limit, and raw bucket. The endpoint must not embed
credentials, a query string, or fragment. Secrets belong in caller-supplied headers or
environment-backed configuration and must never be committed.

Each successful page must contain an array at `records_path` and a string, integer, or
null at `next_cursor_path`. Null ends pagination. Empty/repeated cursors and a page count
beyond `max_pages` fail safely. When `expected_fields` is set, every record must contain
exactly those keys. JSON scalar, array, object, and null values are preserved for the
normalization layer rather than coerced by the adapter.

## Replay and identity

Every HTTP response body—including 429, 5xx, malformed JSON, and invalid shapes—is
written with `upload_if_absent` before status handling, JSON parsing, or normalization.
Objects use deterministic keys under:

```text
s3://raw/http/<source>/responses/page-<number>/attempt-<attempt>-<status>-<sha256>.json
```

If persistence fails, parsing does not proceed. The snapshot metadata contains total
response bytes and an aggregate SHA-256 derived from the ordered page checksums. Its
UUID binds endpoint, aggregate checksum, and record count. Batch UUIDv5 values also bind
the exact field contract and record range, so identical replay produces stable IDs.

## Failure and audit policy

HTTP 429 and 5xx responses, connection failures, and timeouts retry up to
`max_attempts` with bounded exponential backoff. Other non-2xx statuses fail immediately.
The per-request timeout and page cap prevent unbounded work.

`request_metadata` exposes method, query-free URL, page/attempt, status, duration,
parameter names, non-sensitive header names, raw object URI, and transport error type.
It never stores parameter values or header values, and omits authorization, cookie,
API-key, proxy-authorization, and auth-token header names. Applications must preserve
this rule when forwarding metadata to logs or PostgreSQL.

Unit tests use a loopback-only deterministic HTTP server. They cover cursors, bounded
batches, empty results, malformed JSON, wrong response shapes, 429/5xx retry behavior,
non-retryable status, deterministic timeout failures, raw persistence, stable IDs, and
secret-free request metadata. No external API or production payload is used.
