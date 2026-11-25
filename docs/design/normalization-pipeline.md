# Normalization and transformation pipeline (Day 11)

The transformation layer starts only after an adapter has produced a complete
`ExtractedBatch`. It does not perform source discovery or extraction, so connector,
timeout and source-shape failures remain extraction failures. `OrderTransformer`
handles row mapping only; `TransformationPipeline` isolates bad rows and publishes the
accepted and rejected outcomes.

## Explicit order mapping

Each source receives an immutable `OrderMapping` that names the source fields for order
ID, customer ID, event timestamp, amount, currency and optional status/notes. The mapping
also defines:

- `uuid` validation or deterministic UUIDv5 generation for source order identifiers;
- an ordered list of `iso8601` or `datetime.strptime` timestamp formats;
- explicit source-to-canonical status aliases;
- explicit source-to-ISO currency aliases;
- a fixed canonical `source_system` value.

Timestamps must include an offset and are normalized to UTC. UUIDv5 values bind source
system and trimmed source identifier. The canonical contract trims customer identifiers,
rejects floating-point amounts, accepts only exact integer/decimal-string/`Decimal`
inputs, enforces two fractional digits without implicit rounding, uppercases three-letter
currencies and permits only canonical statuses. `ingested_at` is supplied by the caller
and must be timezone-aware; the pipeline never reads the current clock. This makes a
rerun with the same extraction batch and ingestion timestamp deterministic.

## Rejections and counts

Each row is transformed independently. `RecordValidationError` becomes a `RejectedRow`
containing the physical source line, original extracted values and per-field reasons.
Rejected rows are deterministic newline-delimited JSON stored in the `rejected` bucket:

```text
source=<source>/logical_date=YYYY-MM-DD/batch_id=<uuid>/
  rejections/rejected-<sha256-prefix>.jsonl
```

Rows are ordered exactly as extracted, and reason keys and JSON keys are sorted. Common
non-JSON values such as timestamps, decimals, UUIDs and bytes have explicit stable
representations. If no row fails, no rejected object is written.

`TransformationResult` captures input, accepted and rejected counts and enforces
`input = accepted + rejected`. It also returns canonical records, structured rejected
rows, staging object references and the optional rejected object reference.

## Staging Parquet publication

`MinioParquetPublisher` uses the canonical Arrow orders schema, Zstandard compression,
dictionary encoding and the existing anti-tiny-file planner. Accepted records are split
into deterministic `part-<number>-<content-fingerprint>.parquet` objects below the
standard `staging` batch data prefix. Immutable `upload_if_absent` writes make identical
reruns idempotent and detect conflicting content.

Rejected output is persisted before staging publication. Object-store or Parquet errors
raise `TransformationPublicationError` and are never mislabeled as row failures. A later
orchestrator can retry publication using the same batch and `ingested_at`; watermark or
run success must only advance after this pipeline returns successfully.

Unit tests cover mappings, UUID generation, timestamp offsets, decimals, categories,
missing/malformed values, rejection payloads, count invariants, publication failures and
deterministic reruns. The Parquet schema/compression test runs when native PyArrow Parquet
support is available on the host.
