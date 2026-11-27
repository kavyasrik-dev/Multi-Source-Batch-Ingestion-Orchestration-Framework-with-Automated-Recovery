"""Small parameterized metadata repository with composable transactions."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


class DuplicateBatch(ValueError):
    """The source already owns this logical batch key."""


class StateConflict(ValueError):
    """The record is missing or another worker already changed its status."""


class StaleCheckpoint(ValueError):
    """Checkpoint positions must advance strictly when replacing an existing checkpoint."""


class ManifestConflict(ValueError):
    """Manifest registration differs from the batch's immutable identity or prior reference."""


class WatermarkConflict(ValueError):
    """The source watermark is stale or would move backwards."""


class ValidationConflict(ValueError):
    """A validation rule attempt was already registered with different content."""


class MetadataRepository:
    """Use a dedicated connection, preferably autocommit=True.

    Each method commits atomically unless enclosed in conn.transaction(), in which
    case it uses a savepoint and leaves the outer commit/rollback to the caller.
    Do not share a connection between independent concurrent workers.
    """

    TABLES = frozenset(
        {
            "source_config",
            "ingestion_run",
            "batch_manifest",
            "checkpoint",
            "validation_result",
            "recovery_event",
        }
    )

    def __init__(self, conn: psycopg.Connection, schema: str = "metadata") -> None:
        self.conn, self.schema = conn, schema

    @contextmanager
    def _cursor(self) -> Iterator[psycopg.Cursor]:
        with self.conn.transaction(), self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(
                    sql.Identifier(self.schema)
                )
            )
            yield cur

    def get(self, table: str, record_id: UUID) -> dict | None:
        """Fetch one metadata record; table names are explicitly allowlisted."""
        if table not in self.TABLES:
            raise ValueError("Unknown metadata table")
        with self._cursor() as cur:
            cur.execute(
                sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier(table)),
                (record_id,),
            )
            return cur.fetchone()

    def create_source(
        self, name: str, connector_type: str, config: dict[str, Any] | None = None
    ) -> dict:
        """Create source configuration; store secret references, never credentials."""
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO source_config(id, name, connector_type, config)
                VALUES (%s, %s, %s, %s) RETURNING *""",
                (uuid4(), name, connector_type, Jsonb(config if config is not None else {})),
            )
            return cur.fetchone()

    def update_source(self, source_id: UUID, *, config: dict[str, Any], enabled: bool) -> dict:
        """Replace a source's public configuration and enabled flag."""
        with self._cursor() as cur:
            cur.execute(
                """UPDATE source_config SET config = %s, enabled = %s
                WHERE id = %s RETURNING *""",
                (Jsonb(config), enabled, source_id),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(source_id)
            return row

    def delete_source(self, source_id: UUID) -> bool:
        """Delete only an unused source; foreign keys protect ingestion history."""
        with self._cursor() as cur:
            cur.execute("DELETE FROM source_config WHERE id = %s", (source_id,))
            return cur.rowcount == 1

    def list_sources(self, *, enabled_only: bool = True) -> tuple[dict, ...]:
        """List source definitions deterministically, excluding disabled rows by default."""
        with self._cursor() as cur:
            query = "SELECT * FROM source_config"
            if enabled_only:
                query += " WHERE enabled"
            query += " ORDER BY name ASC, id ASC"
            cur.execute(query)
            return tuple(cur.fetchall())

    def create_run(
        self, source_id: UUID, logical_batch_key: str, object_uri: str, *, record_count: int = 0
    ) -> tuple[dict, dict]:
        """Atomically register a run and its first batch, rejecting duplicate claims."""
        try:
            with self._cursor() as cur:
                # Serialize against source disabling without blocking other run creators.
                cur.execute(
                    "SELECT enabled FROM source_config WHERE id = %s FOR SHARE", (source_id,)
                )
                source = cur.fetchone()
                if source is None or not source["enabled"]:
                    raise ValueError("Source is missing or disabled")
                cur.execute(
                    "INSERT INTO ingestion_run(id, source_id) VALUES (%s, %s) RETURNING *",
                    (uuid4(), source_id),
                )
                run = cur.fetchone()
                cur.execute(
                    """INSERT INTO batch_manifest
                    (id, run_id, source_id, logical_batch_key, object_uri, record_count)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING *""",
                    (uuid4(), run["id"], source_id, logical_batch_key, object_uri, record_count),
                )
                return run, cur.fetchone()
        except UniqueViolation as exc:
            if exc.diag.constraint_name == "batch_logical_key":
                raise DuplicateBatch(logical_batch_key) from exc
            raise

    def transition(self, table: str, record_id: UUID, expected: str, new: str) -> dict:
        """Compare-and-set status; database triggers enforce legal transition edges."""
        if table not in {"ingestion_run", "batch_manifest", "recovery_event"}:
            raise ValueError("Table has no mutable status")
        with self._cursor() as cur:
            cur.execute(
                sql.SQL(
                    "UPDATE {} SET status = %s WHERE id = %s AND status = %s RETURNING *"
                ).format(sql.Identifier(table)),
                (new, record_id, expected),
            )
            row = cur.fetchone()
            if row is None:
                raise StateConflict(str(record_id))
            return row

    def save_checkpoint(
        self, batch_id: UUID, step: str, position: int, payload: dict[str, Any] | None = None
    ) -> dict:
        """Insert a checkpoint or advance strictly; reject stale/equal writes."""
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO checkpoint(id, batch_id, step, position, payload)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, step) DO UPDATE
                SET position = EXCLUDED.position, payload = EXCLUDED.payload
                WHERE checkpoint.position < EXCLUDED.position RETURNING *""",
                (uuid4(), batch_id, step, position, Jsonb(payload if payload is not None else {})),
            )
            row = cur.fetchone()
            if row is None:
                raise StaleCheckpoint(step)
            return row

    def load_source_watermark(self, source_id: UUID) -> dict:
        """Create and lock-free read a nullable initial watermark for a configured source."""
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO source_watermark(source_id)
                SELECT id FROM source_config WHERE id = %s
                ON CONFLICT (source_id) DO NOTHING""",
                (source_id,),
            )
            cur.execute("SELECT * FROM source_watermark WHERE source_id = %s", (source_id,))
            row = cur.fetchone()
            if row is None:
                raise KeyError(source_id)
            return row

    def advance_source_watermark(
        self,
        source_id: UUID,
        *,
        expected_version: int,
        watermark: datetime,
        tie_breaker: object,
        publication_uri: str,
    ) -> dict:
        """Compare-and-set a watermark only after a caller reports successful publication."""
        if watermark.tzinfo is None or watermark.utcoffset() is None:
            raise ValueError("watermark must be timezone-aware")
        if tie_breaker is None or not publication_uri.strip():
            raise ValueError("tie_breaker and publication_uri are required")
        with self._cursor() as cur:
            cur.execute(
                """SELECT * FROM source_watermark
                WHERE source_id = %s FOR UPDATE""",
                (source_id,),
            )
            current = cur.fetchone()
            if current is None or current["version"] != expected_version:
                raise WatermarkConflict(str(source_id))
            current_position = (current["watermark"], current["tie_breaker"])
            candidate = (watermark, tie_breaker)
            if current["watermark"] is not None and candidate <= current_position:
                raise WatermarkConflict("watermark must advance strictly")
            cur.execute(
                """UPDATE source_watermark
                SET watermark = %s, tie_breaker = %s, version = version + 1,
                    publication_uri = %s
                WHERE source_id = %s AND version = %s
                RETURNING *""",
                (
                    watermark,
                    Jsonb(tie_breaker),
                    publication_uri,
                    source_id,
                    expected_version,
                ),
            )
            row = cur.fetchone()
            if row is None:
                raise WatermarkConflict(str(source_id))
            return row

    def register_manifest(
        self,
        batch_id: UUID,
        *,
        source: str,
        logical_date: date,
        object_uri: str,
        record_count: int,
        content_checksum_sha256: str,
        content_bytes: int,
        manifest_uri: str,
        manifest_checksum_sha256: str,
    ) -> dict:
        """Set an exact manifest reference once, or return the identical registration."""
        with self._cursor() as cur:
            cur.execute(
                """SELECT batch.*, source.name AS source_name
                FROM batch_manifest AS batch
                JOIN source_config AS source ON batch.source_id = source.id
                WHERE batch.id = %s
                FOR UPDATE OF batch""",
                (batch_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise KeyError(batch_id)
            expected = {
                "source_name": source,
                "object_uri": object_uri,
                "record_count": record_count,
            }
            if any(row[field] != value for field, value in expected.items()):
                raise ManifestConflict(str(batch_id))
            registration = {
                "logical_date": logical_date,
                "content_checksum_sha256": content_checksum_sha256,
                "content_bytes": content_bytes,
                "manifest_uri": manifest_uri,
                "manifest_checksum_sha256": manifest_checksum_sha256,
            }
            if row["manifest_uri"] is not None:
                if all(row[field] == value for field, value in registration.items()):
                    row.pop("source_name")
                    return row
                raise ManifestConflict(str(batch_id))
            cur.execute(
                """UPDATE batch_manifest
                SET logical_date = %s,
                    content_checksum_sha256 = %s,
                    content_bytes = %s,
                    manifest_uri = %s,
                    manifest_checksum_sha256 = %s
                WHERE id = %s
                RETURNING *""",
                (
                    logical_date,
                    content_checksum_sha256,
                    content_bytes,
                    manifest_uri,
                    manifest_checksum_sha256,
                    batch_id,
                ),
            )
            return cur.fetchone()

    def add_validation(
        self,
        batch_id: UUID,
        rule_name: str,
        status: str,
        checked_count: int,
        failed_count: int,
        *,
        attempt: int = 1,
        details: dict[str, Any] | None = None,
    ) -> dict:
        """Append an immutable validation outcome for one rule attempt."""
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO validation_result
                (id, batch_id, rule_name, attempt, status, checked_count, failed_count, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *""",
                (
                    uuid4(),
                    batch_id,
                    rule_name,
                    attempt,
                    status,
                    checked_count,
                    failed_count,
                    Jsonb(details if details is not None else {}),
                ),
            )
            return cur.fetchone()

    def register_validation_summary(
        self,
        batch_id: UUID,
        rule_name: str,
        status: str,
        checked_count: int,
        failed_count: int,
        *,
        attempt: int = 1,
        details: dict[str, Any] | None = None,
    ) -> dict:
        """Register an immutable summary idempotently and reject conflicting reruns."""
        values = details if details is not None else {}
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO validation_result
                (id, batch_id, rule_name, attempt, status, checked_count, failed_count, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, rule_name, attempt) DO NOTHING
                RETURNING *""",
                (
                    uuid4(),
                    batch_id,
                    rule_name,
                    attempt,
                    status,
                    checked_count,
                    failed_count,
                    Jsonb(values),
                ),
            )
            row = cur.fetchone()
            if row is not None:
                return row
            cur.execute(
                """SELECT * FROM validation_result
                WHERE batch_id = %s AND rule_name = %s AND attempt = %s
                FOR UPDATE""",
                (batch_id, rule_name, attempt),
            )
            row = cur.fetchone()
            expected = {
                "status": status,
                "checked_count": checked_count,
                "failed_count": failed_count,
                "details": values,
            }
            if row is None or any(row[key] != value for key, value in expected.items()):
                raise ValidationConflict(f"{batch_id}:{rule_name}:{attempt}")
            return row

    def add_recovery(
        self,
        batch_id: UUID,
        run_id: UUID,
        attempt: int,
        action: str,
        details: dict[str, Any] | None = None,
    ) -> dict:
        """Register a pending recovery attempt associated with its batch's run."""
        with self._cursor() as cur:
            cur.execute(
                """INSERT INTO recovery_event
                (id, batch_id, run_id, attempt, action, status, details)
                VALUES (%s, %s, %s, %s, %s, 'pending', %s) RETURNING *""",
                (
                    uuid4(),
                    batch_id,
                    run_id,
                    attempt,
                    action,
                    Jsonb(details if details is not None else {}),
                ),
            )
            return cur.fetchone()
