# PostgreSQL source adapter (Day 9)

`PostgresAdapter` reads one configured table through a dedicated PostgreSQL connection
and implements the shared discovery, metadata, extraction and bounded-batch contract.
The source URL exposed to orchestration contains only schema/table names; credentials
come from `POSTGRES_SOURCE_DATABASE_URL` or another caller-managed secret reference.

## Source contract and ordering

The configured projection must include a non-null `timestamptz` watermark column and a
non-null, immutable integer or text tie-breaker. The pair must be unique and is ordered
ascending. Equal timestamps are therefore safe: a later `(timestamp, id)` tuple remains
extractable. Identifiers are composed with Psycopg SQL objects and values are always
bound parameters.

`PsycopgSourceReader` starts every window in a `REPEATABLE READ READ ONLY` transaction.
It applies a hard `max_window_records` limit, closes its dedicated connection, and wraps
connection/database failures without changing metadata state. A production source role
should also have database-level `SELECT` permission only for its dedicated schema/table.

## Watermarks and late data

Migration 0004 adds `source_watermark`, keyed by `source_config.id`, with a nullable
initial timestamp/tie-breaker pair and optimistic-lock version. A null pair means the
first window reads from the beginning. With no lookback, later windows use strict tuple
comparison. With `lookback_seconds > 0`, the adapter rereads the overlap interval so a
row arriving late with an older business timestamp is not skipped. Overlap rows may be
replayed intentionally; deterministic window/batch IDs and downstream uniqueness must
make publication idempotent.

Rows are validated for exact columns, timezone-aware timestamps, configured tie-breaker
type and strictly increasing order before the window can be published. The candidate
high-water position is the maximum of the stored position and validated rows, so an
overlap-only window can never regress state.

## Publication gate

Reading, metadata inspection and batch iteration never advance the watermark. After the
caller has validated every record and durably published the output, it calls
`acknowledge_published(window, validation_passed=True, publication_uri=...)`.
`MetadataRepository.advance_source_watermark` then compares the captured version under
a row lock and advances strictly in the same transaction. Failed validation, missing
publication references, connection loss, stale workers and backward/equal candidates
leave the stored watermark unchanged.

Unit tests cover initial null state, equal timestamps, overlap/late rows, stable batches,
empty windows, invalid rows, connection loss, stale updates and publication gating. The
opt-in integration test creates a dedicated source schema/table and runs the full query
and metadata workflow when `TEST_DATABASE_URL` points to a disposable test database.
