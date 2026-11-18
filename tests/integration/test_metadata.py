"""Real PostgreSQL tests. Set TEST_DATABASE_URL to opt into a dedicated test database."""

import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.errors import (
    CheckViolation,
    DivisionByZero,
    ForeignKeyViolation,
    RestrictViolation,
    UniqueViolation,
)

from app.common.metadata.migrate import MIGRATIONS, migrate
from app.common.metadata.repository import (
    DuplicateBatch,
    ManifestConflict,
    MetadataRepository,
    StaleCheckpoint,
    StateConflict,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def database():
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL to run real PostgreSQL integration tests")
    schema = "test_metadata_" + uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            migrate(conn, schema)
            yield conn, schema, dsn
        finally:
            conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
def repo(database):
    conn, schema, _ = database
    return MetadataRepository(conn, schema)


def seed(repo):
    source = repo.create_source("source-" + uuid4().hex, "http", {"secret_ref": "local/source"})
    run, batch = repo.create_run(source["id"], "daily-2026-11-16", "s3://raw/example.json")
    return source, run, batch


def test_migrations_reapply_and_rollback(database):
    conn, schema, _ = database
    assert migrate(conn, schema) == 3
    with pytest.raises(ValueError, match="Downgrade"):
        migrate(conn, schema, target=0)
    assert migrate(conn, schema, target=0, allow_downgrade=True) == 0
    assert (
        conn.execute("SELECT to_regclass(%s)", (schema + ".source_config",)).fetchone()[0] is None
    )
    assert migrate(conn, schema) == 3


def test_failed_migration_rolls_back_all_pending_ddl(database, tmp_path):
    conn, schema, _ = database
    for path in MIGRATIONS.glob("*.sql"):
        shutil.copy(path, tmp_path)
    (tmp_path / "0004_broken.up.sql").write_text(
        "CREATE TABLE must_rollback(id integer); SELECT 1 / 0;", encoding="utf-8"
    )
    (tmp_path / "0004_broken.down.sql").write_text("DROP TABLE must_rollback;", encoding="utf-8")
    with pytest.raises(DivisionByZero):
        migrate(conn, schema, directory=tmp_path)
    assert (
        conn.execute("SELECT to_regclass(%s)", (schema + ".must_rollback",)).fetchone()[0] is None
    )
    assert migrate(conn, schema) == 3


def test_migration_checksum_tampering_is_rejected(database, tmp_path):
    conn, schema, _ = database
    for path in MIGRATIONS.glob("*.sql"):
        shutil.copy(path, tmp_path)
    with (tmp_path / "0001_metadata.up.sql").open("a", encoding="utf-8") as file:
        file.write("\n-- changed\n")
    with pytest.raises(ValueError, match="has changed"):
        migrate(conn, schema, directory=tmp_path)


def test_source_crud_and_history_protection(repo):
    source = repo.create_source("unused", "file")
    assert repo.get("source_config", source["id"])["enabled"] is True
    updated = repo.update_source(source["id"], config={"path": "fixtures"}, enabled=False)
    assert updated["config"] == {"path": "fixtures"}
    assert updated["updated_at"] >= updated["created_at"]
    with pytest.raises(ValueError, match="disabled"):
        repo.create_run(source["id"], "disabled", "s3://raw/x")
    assert repo.delete_source(source["id"])
    assert not repo.delete_source(source["id"])
    assert repo.get("source_config", source["id"]) is None
    source, _, _ = seed(repo)
    with pytest.raises((ForeignKeyViolation, RestrictViolation)):
        repo.delete_source(source["id"])


def test_all_metadata_records_and_state_transitions(repo):
    source, run, batch = seed(repo)
    assert run["source_id"] == source["id"]
    running = repo.transition("ingestion_run", run["id"], "created", "running")
    assert running["started_at"] is not None
    with pytest.raises(StateConflict):
        repo.transition("ingestion_run", run["id"], "created", "running")
    failed = repo.transition("ingestion_run", run["id"], "running", "failed")
    assert failed["finished_at"] is not None
    retry = repo.transition("ingestion_run", run["id"], "failed", "running")
    assert retry["finished_at"] is None
    assert retry["started_at"] == running["started_at"]
    result = repo.add_validation(batch["id"], "schema", "passed", 10, 0)
    assert repo.get("validation_result", result["id"])["checked_count"] == 10
    recovery = repo.add_recovery(batch["id"], run["id"], 1, "retry")
    repo.transition("recovery_event", recovery["id"], "pending", "running")
    repo.transition("recovery_event", recovery["id"], "running", "succeeded")
    assert repo.get("recovery_event", recovery["id"])["status"] == "succeeded"
    repo.transition("batch_manifest", batch["id"], "registered", "staged")
    repo.transition("batch_manifest", batch["id"], "staged", "curated")
    done = repo.transition("ingestion_run", run["id"], "running", "succeeded")
    assert done["finished_at"] >= done["started_at"]
    with pytest.raises(CheckViolation):
        repo.transition("ingestion_run", run["id"], "succeeded", "running")
    with pytest.raises(CheckViolation):
        repo.transition("batch_manifest", batch["id"], "curated", "staged")


def test_duplicate_batch_rolls_back_new_run_and_connection_remains_usable(repo, database):
    source, _, _ = seed(repo)
    with pytest.raises(DuplicateBatch):
        repo.create_run(source["id"], "daily-2026-11-16", "s3://raw/duplicate")
    conn, schema, _ = database
    count = conn.execute(
        sql.SQL("SELECT count(*) FROM {}.ingestion_run").format(sql.Identifier(schema))
    ).fetchone()[0]
    assert count == 1
    assert repo.create_run(source["id"], "different-key", "s3://raw/new")[0]["status"] == "created"


def test_outer_transaction_rolls_back_every_repository_write(repo):
    source = repo.create_source("transaction-source", "http")
    run_id = batch_id = None
    with pytest.raises(RuntimeError, match="abort"), repo.conn.transaction():
        run, batch = repo.create_run(source["id"], "rollback", "s3://raw/x")
        run_id, batch_id = run["id"], batch["id"]
        repo.save_checkpoint(batch_id, "extract", 10)
        repo.transition("ingestion_run", run_id, "created", "running")
        raise RuntimeError("abort")
    assert repo.get("ingestion_run", run_id) is None
    assert repo.get("batch_manifest", batch_id) is None


def test_checkpoints_reject_stale_and_equal_positions(repo):
    _, _, batch = seed(repo)
    first = repo.save_checkpoint(batch["id"], "extract", 0)
    advanced = repo.save_checkpoint(batch["id"], "extract", 10, {"cursor": "next"})
    assert advanced["id"] == first["id"]
    for position in (0, 10):
        with pytest.raises(StaleCheckpoint):
            repo.save_checkpoint(batch["id"], "extract", position)
    assert repo.get("checkpoint", first["id"])["position"] == 10


def test_manifest_registration_is_idempotent_and_conflict_safe(repo):
    source, _, batch = seed(repo)
    values = {
        "source": source["name"],
        "logical_date": date(2026, 11, 16),
        "object_uri": batch["object_uri"],
        "record_count": batch["record_count"],
        "content_checksum_sha256": "a" * 64,
        "content_bytes": 123,
        "manifest_uri": f"s3://raw/manifests/{batch['id']}.json",
        "manifest_checksum_sha256": "b" * 64,
    }
    first = repo.register_manifest(batch["id"], **values)
    second = repo.register_manifest(batch["id"], **values)
    assert first["manifest_uri"] == second["manifest_uri"]
    assert first["updated_at"] == second["updated_at"]
    with pytest.raises(ManifestConflict):
        repo.register_manifest(batch["id"], **{**values, "manifest_checksum_sha256": "c" * 64})


def test_constraints_and_composite_relationships(repo, database):
    source, run, batch = seed(repo)
    other = repo.create_source("other", "http")
    other_run, _ = repo.create_run(other["id"], "other-key", "s3://raw/other")
    with pytest.raises(ForeignKeyViolation):
        repo.add_recovery(batch["id"], other_run["id"], 1, "retry")
    with pytest.raises(CheckViolation):
        repo.add_validation(batch["id"], "counts", "passed", 1, 2)
    with pytest.raises(CheckViolation):
        repo.save_checkpoint(batch["id"], "extract", -1)
    with pytest.raises(CheckViolation):
        repo.create_run(source["id"], "negative", "s3://raw/x", record_count=-1)
    repo.add_validation(batch["id"], "unique-rule", "failed", 1, 1)
    with pytest.raises(UniqueViolation):
        repo.add_validation(batch["id"], "unique-rule", "passed", 1, 0)
    repo.add_recovery(batch["id"], run["id"], 1, "retry")
    with pytest.raises(UniqueViolation):
        repo.add_recovery(batch["id"], run["id"], 1, "retry")
    conn, schema, _ = database
    with pytest.raises(ForeignKeyViolation), conn.transaction():
        conn.execute(
            sql.SQL("""INSERT INTO {}.batch_manifest
            (id, run_id, source_id, logical_batch_key, object_uri)
            VALUES (%s, %s, %s, 'wrong-source', 's3://raw/x')""").format(sql.Identifier(schema)),
            (uuid4(), run["id"], other["id"]),
        )
    with pytest.raises(CheckViolation), conn.transaction():
        conn.execute(
            sql.SQL("UPDATE {}.ingestion_run SET status = 'unknown' WHERE id = %s").format(
                sql.Identifier(schema)
            ),
            (run["id"],),
        )


def test_simultaneous_duplicate_claims_have_one_winner(repo, database):
    source = repo.create_source("concurrent", "http")
    _, schema, dsn = database
    barrier = Barrier(2)

    def claim():
        with psycopg.connect(dsn, autocommit=True) as conn:
            worker = MetadataRepository(conn, schema)
            barrier.wait(timeout=10)
            try:
                worker.create_run(source["id"], "same-batch", "s3://raw/x")
                return "created"
            except DuplicateBatch:
                return "duplicate"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(lambda _: claim(), range(2))) == ["created", "duplicate"]


def test_simultaneous_state_transitions_have_one_winner(repo, database):
    _, run, _ = seed(repo)
    repo.transition("ingestion_run", run["id"], "created", "running")
    _, schema, dsn = database
    barrier = Barrier(2)

    def finish(status):
        with psycopg.connect(dsn, autocommit=True) as conn:
            worker = MetadataRepository(conn, schema)
            barrier.wait(timeout=10)
            try:
                worker.transition("ingestion_run", run["id"], "running", status)
                return "updated"
            except StateConflict:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(finish, ["succeeded", "failed"])) == ["conflict", "updated"]
