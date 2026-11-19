# Source adapter contract and CSV adapter (Day 7)

`SourceAdapter` defines four operations: deterministic discovery, checksum-backed
metadata, streaming extraction and bounded batch iteration. Adapters return shared
immutable types so ingestion orchestration does not depend on source-specific clients.

| Type | Meaning |
| --- | --- |
| `SourceObject` | Canonical locator and display name discovered by an adapter |
| `SourceMetadata` | UUID identity, SHA-256, bytes, UTC modification time and media type |
| `SourceRecord` | Validated source values and physical source line |
| `ExtractedBatch` | Non-empty bounded tuple, stable UUID, start offset and source metadata |

Callers should use `iter_batches` and fully consume it. `extract` remains available for
record streaming, but its final source-change check runs only when the iterator reaches
the end. Batch IDs use UUIDv5 over the source identity, configured header contract and
record range. An unchanged file and configuration produce identical IDs across retry;
changed content, path, schema contract or record boundaries produce different IDs.

## CSV configuration

`CSVAdapterConfig` accepts one or more file/directory paths, a one-character delimiter,
Python codec name, header mode, optional exact headers, and recursive directory
discovery. `from_path` is the convenience constructor for one path. Directory discovery
includes `.csv` files only and sorts canonical resolved paths. Supplying the same file
directly and through a directory is rejected as duplicate input.

With `has_header=True`, the physical first record is the header. Blank/duplicate names
and any mismatch with `expected_headers` fail before data is yielded. Empty files and
header-only files produce no batches. With `has_header=False`, `expected_headers` is
required and the first physical record is data.

Parsing uses Python's strict CSV reader with `newline=""`. Wrong field counts report
the file, physical line, expected/received counts and offending row. CSV syntax and
Unicode decoding errors are wrapped with file/line or codec/byte context. Values remain
strings, including empty strings; canonical type conversion belongs to the Day 5 data
contract. The adapter does not silently trim fields or infer delimiters/types.

Metadata hashes the complete file with SHA-256 in 1 MiB chunks. File identity binds the
canonical file URI, byte count and checksum. Extraction checks size/modification time
and rehashes before completion to detect changes. Files should be immutable while an
ingestion run is reading them. A process crash or source change should discard partial
downstream output and retry discovery.

Fixtures under `tests/fixtures/csv` are tiny synthetic records only. Unit tests cover
empty/header-only input, malformed row diagnostics, exact headers, Unicode decoding,
custom delimiter, headerless files, duplicates, stable IDs, content changes and batch
bounds. No production or raw customer data belongs in repository fixtures.
