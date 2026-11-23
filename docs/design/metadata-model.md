# PostgreSQL metadata model (Day 4)

The `metadata` schema is independent of application payload data. SQL migrations
are packaged under `app/common/metadata/migrations` so installed distributions can
run them. PostgreSQL 17 and Psycopg 3.2.10 are the current runtime baseline.
Migrations are explicit; the Compose database volume is never automatically reset.

## Tables and identity

All records have application-generated UUID primary keys and UTC-aware
`timestamptz` creation timestamps. Mutable records also have trigger-maintained
`updated_at`. JSON metadata must be an object. Store secret references in source
configuration and diagnostic details, never passwords or tokens.

| Table | Relationship and uniqueness | Purpose |
| --- | --- | --- |
| `source_config` | unique nonblank name | Connector kind, public config, enabled flag |
| `ingestion_run` | source FK; unique `(id, source_id)` | Execution status and start/finish times |
| `batch_manifest` | composite run/source FK; unique logical key and manifest URI | Batch identity, object/manifest integrity and processing state |
| `checkpoint` | batch FK; unique `(batch_id, step)` | Resume position and cursor payload |
| `validation_result` | batch FK; unique `(batch_id, rule_name, attempt)` | Append-only rule outcome and checked/failed counts |
| `recovery_event` | composite batch/run FK; unique `(batch_id, attempt)` | Recovery action, state and diagnostic details |
| `source_watermark` | source PK/FK; optimistic-lock version | Published incremental position and durable output reference |

Foreign keys restrict deletes, preserving history. The repository only exposes
deletion for unused sources; retire used sources with `enabled=False`. No blanket
delete or arbitrary table update API is provided. Validation outcomes and recovery
attempt identity are append-only by repository convention, not database privileges.

A logical batch key is an exact, case-sensitive source-specific identifier such as
a source partition plus extraction window. The caller must canonicalize it. The
uniqueness constraint applies across all runs and states, including failed runs.
Retries resume the existing run and batch; they do not register a second logical batch.
Each repository-created run currently registers one initial batch atomically.

Counts, positions and attempts have nonnegative/positive checks. Passed validations
cannot have failed rows, and failed_count cannot exceed checked_count. Composite
foreign keys prevent associating a batch with the wrong source or recovery with
the wrong run. Indexes support source history, active-run scans, batch status,
validation failures and pending recovery. Unique constraints already index the
batch, checkpoint, validation and recovery lookup prefixes.

## State machines and transactions

Database triggers reject illegal state edges, even for direct SQL clients:

- Runs: `created -> running | cancelled`; `running -> succeeded | failed | cancelled`;
  `failed -> running | cancelled`. Succeeded and cancelled are terminal.
- Batches: `registered -> staged | rejected`; `staged -> curated | rejected`.
- Recovery: `pending -> running -> succeeded | failed`. Another attempt is a new row.

Run inserts must start created. Triggers assign started_at on first start and
finished_at on terminal/failed status; retry clears finished_at but retains the
original start. Timestamp checks prevent contradictory status/time combinations.
Validation statuses are passed, failed, error and skipped.

Migration 3 adds logical date, content SHA-256/bytes, manifest URI and manifest
SHA-256 as an all-or-none registration. Historical rows can remain unregistered.
Migration 4 adds the nullable timestamp/tie-breaker watermark pair used by incremental
PostgreSQL sources. Repository updates lock the row, compare its version and reject
backward, equal or stale advancement.

`MetadataRepository` uses bound values, allowlisted table names and quoted schema
identifiers. Each method opens a transaction (or a savepoint inside a caller's
transaction). Prefer a dedicated `psycopg.connect(..., autocommit=True)` connection
per worker; the caller owns any outer transaction. A source share lock serializes
run creation against source disabling. Duplicate registration rolls back both the
new run and manifest and raises `DuplicateBatch`, leaving the connection usable.
Uniqueness is enforced by PostgreSQL, including concurrent workers.

`transition(table, id, expected, new)` uses a single compare-and-set UPDATE. A
missing row or stale expected status raises `StateConflict`; an invalid edge raises
Psycopg CheckViolation. Checkpoint upserts strictly advance position and raise
`StaleCheckpoint` on lower or equal writes; their previous payload is preserved.
Direct SQL can bypass the checkpoint monotonicity convention, so use the repository.

## Migrations and local use

Set `METADATA_DATABASE_URL` in your process environment to the local Compose
PostgreSQL connection string. The migration CLI does not load `.env` automatically.
Do not put connection URLs on command lines or in tracked files. Example operations:

```sh
docker compose --env-file .env -f infrastructure/docker/compose.yaml up -d postgres --wait
python -m app.common.metadata.migrate
python -m app.common.metadata.migrate --target 1 --allow-downgrade
```

Downgrades are destructive administrative operations. Version 4 removes source
watermarks, version 3 removes manifest references, version 2 removes guards and indexes,
and version 1 rollback deletes the original six metadata tables and their contents.
Take backups before any downgrade. A checksummed ledger and advisory transaction
lock serialize runners, detect edited migration files and reject gaps. Pending
migrations and ledger changes commit together; failure rolls back the entire call.
Rollback requires an explicit flag. A target of 0 leaves the empty migration ledger.

```python
import os
import psycopg
from app.common.metadata.repository import MetadataRepository

with psycopg.connect(os.environ["METADATA_DATABASE_URL"], autocommit=True) as conn:
    repo = MetadataRepository(conn)
    source = repo.create_source("example", "http", {"secret_ref": "sources/example"})
    run, batch = repo.create_run(source["id"], "example-window", "s3://raw/example.json")
    repo.transition("ingestion_run", run["id"], "created", "running")
```

## Integration tests

Create a dedicated test database on Compose PostgreSQL, then set TEST_DATABASE_URL
to that database in your shell. The test user must be allowed to create/drop
schemas. Tests create random `test_metadata_*` schemas and drop only those schemas
afterwards. They do not drop the database or touch the default metadata schema.

```sh
docker compose --env-file .env -f infrastructure/docker/compose.yaml exec postgres sh -ec 'createdb -U "$POSTGRES_USER" metadata_test'
python -m pytest tests/integration/test_metadata.py
```

The suite tests migration upgrade/reapply/downgrade, failed-DDL rollback, checksum
drift, CRUD, constraints, composite relationships, explicit transaction rollback,
duplicate registration rollback, checkpoints, retries and concurrent state/batch
claims. Missing TEST_DATABASE_URL skips database tests explicitly; a configured but
unreachable database fails them. Never report skipped integration tests as passed.

Docker remains unavailable on the authoring machine. The suite is also exercised
against an isolated local PostgreSQL 18.6 cluster as supplemental validation. It must
still be executed against Compose PostgreSQL 17 before that specific requirement
is considered verified.
