# Source registry and batch discovery (Day 10)

`SourceRegistry` is the entry point for loading enabled source definitions, validating
their settings, discovering bounded adapter batches and claiming their logical identity.
It keeps source configuration separate from adapter execution and orchestration.

## Configuration resolution

PostgreSQL `source_config` rows remain authoritative for source ID, name, connector type,
public connector settings and enabled state. `MetadataRepository.list_sources()` returns
enabled rows by default in `(name, id)` order. Disabled metadata rows never reach adapter
construction.

`RegistryConfig` and `RuntimeSourceConfig` provide typed runtime controls. Runtime source
entries can disable an otherwise-enabled source, set `batch_size` and `batch_limit`, and
override connector settings. Runtime overrides win over metadata; typed runtime limits
win over metadata limits; registry defaults apply last. Secret-bearing HTTP headers may
be supplied as runtime overrides and are excluded from dataclass representations. They
must not be stored in `source_config` or committed files.

The parser rejects unknown keys, non-positive limits, unsupported connector types and
missing required values. It creates the concrete CSV, HTTP/JSON or PostgreSQL adapter
configuration during loading, so invalid delimiters, endpoints, codecs, field contracts,
incremental columns and retry bounds fail before network/database/file execution.

## Deterministic discovery

Sources and source objects are sorted before traversal. Each adapter receives its
per-source `batch_size`, and no more than `batch_limit` batches are consumed across all
objects for that source. The registry logical key is `v1:<UUIDv5>` over canonical JSON
containing:

- metadata source UUID and connector type;
- canonical source-object locator;
- stable adapter batch UUID and content SHA-256;
- starting record and record count.

Unchanged source/configuration state therefore produces the same keys on repeated
discovery. A duplicate key emitted within one discovery is treated as an adapter error,
not silently collapsed. Multi-source results are returned in deterministic source,
locator, record-offset and batch-ID order.

## Concurrency-safe registration

`register()` calls `MetadataRepository.create_run()` with the logical key, input locator
and record count. PostgreSQL's unique `(source_id, logical_batch_key)` constraint is the
coordination primitive: concurrent workers may both discover a batch, but only one
transaction creates its run and manifest row. The losing `DuplicateBatch` result becomes
`BatchRegistration(created=False)` and does not create a partial run. No process-local
lock is used.

Unit tests cover disabled sources, strict runtime parsing, invalid metadata, unsupported
connectors, per-source limits, stable repeated discovery, duplicate adapter output,
idempotent registration and deterministic multi-source order. Existing PostgreSQL
integration tests exercise enabled filtering and simultaneous duplicate claims.
