# MinIO objects and batch manifests (Day 6)

The storage adapter is byte-oriented and wraps MinIO SDK 7.2.18. It provides upload,
download, recursive list, existence and content SHA-256 operations. Uploads attach the
SHA-256 as user metadata, but verification hashes downloaded bytes because ETags are
not content checksums for multipart objects and metadata alone is not proof of content.
Download responses are always closed and returned to the connection pool.

Connection, timeout, HTTP and selected retryable S3 failures use bounded exponential
backoff. Upload retry is safe because each attempt creates a new `BytesIO` over the
same immutable bytes. Missing objects raise `ObjectNotFound`; existence returns false
only for missing resources. Permission/configuration errors are not retried. Exhausted
transient failures raise `TransientObjectStoreError` with the object URI.

## Zones and keys

The four data zones use their existing, identically named buckets: `raw`, `staging`,
`curated`, and `rejected`. Every zone uses the same prefix shape:

```text
source=<source>/logical_date=YYYY-MM-DD/batch_id=<uuid>/
├── data/<safe-filename>
└── manifest.v1.json
```

Source names are lowercase letters/digits plus `_` or `-`, at most 63 characters.
Filenames are single safe leaf names. These rules prevent path traversal and naming
collisions caused by lossy slug conversion. Moving a batch between zones preserves
its key and changes only the bucket.

## Manifest v1

The deterministic UTF-8 JSON manifest contains `schema_version`, `source`, `batch_id`,
`logical_date`, `object_key`, `checksum_sha256`, `bytes`, and `record_count`. The
preferred constructor is `BatchManifest.from_stored_object`, which copies checksum
and byte count from the verified upload result. Serialization sorts keys, removes
insignificant whitespace and ends with one newline, producing a stable manifest hash.

Before registering, `ManifestRegistrar` downloads the referenced data object and
verifies its SHA-256 and byte count. It then writes the deterministic manifest key.
If that key exists with identical bytes, the operation reuses it; different bytes
raise `ObjectConflict`. Finally, PostgreSQL records the manifest URI, manifest hash,
content hash, content bytes and logical date on `batch_manifest`.

The database update also requires the existing source name, object URI and record
count to match. An exact repeat returns the same row; any difference raises
`ManifestConflict`. The MinIO write happens before the PostgreSQL transaction because
the systems cannot share a transaction. A database failure can leave an unreferenced,
deterministically named manifest; retrying is safe and completes registration. The
database row is authoritative for whether registration completed.

`upload_if_absent` provides practical idempotency through read/compare/write/verify,
but S3-compatible stores do not make that client-side sequence atomic against two
writers with different content. The PostgreSQL compare-and-set is the authoritative
concurrency guard. Assign one writer per logical batch and treat `ObjectConflict` as
an integrity incident.

Migration 3 adds nullable manifest columns so historical rows remain valid, plus an
all-or-none registration constraint, checksum formats, nonnegative bytes and unique
manifest URI. Rollback removes these references and is destructive metadata loss.

## Tests

Unit tests use an in-memory SDK double to cover uploads, downloads, list/existence,
checksums, missing objects, response cleanup, transient retries, exhausted retries,
invalid keys, exact duplicate manifests and conflicting duplicates. PostgreSQL tests
cover repeat registration and conflicts.

For real Compose MinIO, export `TEST_MINIO_ENDPOINT`, `TEST_MINIO_ACCESS_KEY`,
`TEST_MINIO_SECRET_KEY`, and optionally `TEST_MINIO_SECURE`, then run `make
test-storage`. Tests use a unique prefix in the `raw` bucket and remove only those
objects. Docker is unavailable on the authoring machine, so this opt-in integration
test remains to be run against the Day 3 Compose service.
