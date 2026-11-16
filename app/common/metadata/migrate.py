"""Apply checksummed SQL migrations transactionally; never run automatically at startup."""

import argparse
import hashlib
import os
from pathlib import Path

import psycopg
from psycopg import sql

MIGRATIONS = Path(__file__).with_name("migrations")


def migrate(
    conn: psycopg.Connection,
    schema: str = "metadata",
    *,
    target: int | None = None,
    allow_downgrade: bool = False,
    directory: Path = MIGRATIONS,
) -> int:
    """Atomically reach a version, serializing runners with a transaction advisory lock.

    Rollbacks can destroy data and require explicit allow_downgrade=True.
    A caller-owned outer transaction remains owned by the caller.
    """
    files = sorted(directory.glob("*.up.sql"))
    versions = {}
    for path in files:
        version = int(path.name.split("_", 1)[0])
        if version in versions:
            raise ValueError("Duplicate migration version")
        down = path.with_name(path.name.replace(".up.sql", ".down.sql"))
        up_text, down_text = path.read_text(encoding="utf-8"), down.read_text(encoding="utf-8")
        digest = hashlib.sha256((up_text + "\0" + down_text).encode()).hexdigest()
        versions[version] = (up_text, down_text, digest)
    if list(versions) != list(range(1, len(versions) + 1)):
        raise ValueError("Migration versions must be contiguous starting at 1")
    target = len(versions) if target is None else target
    if target < 0 or target > len(versions):
        raise ValueError("Unknown target migration version")
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"metadata:{schema}",))
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL("SET LOCAL search_path TO {}, pg_catalog").format(sql.Identifier(schema))
        )
        cur.execute("""CREATE TABLE IF NOT EXISTS schema_migration (
            version integer PRIMARY KEY, checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp())""")
        cur.execute("SELECT version, checksum FROM schema_migration ORDER BY version")
        applied = dict(cur.fetchall())
        if list(applied) != list(range(1, len(applied) + 1)):
            raise ValueError("Non-contiguous applied migration history")
        for version, checksum in applied.items():
            if version not in versions or versions[version][2] != checksum:
                raise ValueError(f"Migration {version} is missing or has changed")
        current = len(applied)
        if target < current and not allow_downgrade:
            raise ValueError("Downgrade requires explicit permission")
        for version in range(current, target, -1):
            cur.execute(versions[version][1])
            cur.execute("DELETE FROM schema_migration WHERE version = %s", (version,))
        for version in range(current + 1, target + 1):
            cur.execute(versions[version][0])
            cur.execute(
                "INSERT INTO schema_migration(version, checksum) VALUES (%s, %s)",
                (version, versions[version][2]),
            )
    return target


def main() -> None:
    """Read connection credentials from the environment, never from CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", default="metadata")
    parser.add_argument("--target", type=int)
    parser.add_argument("--allow-downgrade", action="store_true")
    args = parser.parse_args()
    dsn = os.environ.get("METADATA_DATABASE_URL")
    if not dsn:
        parser.error("Set METADATA_DATABASE_URL; .env is not loaded automatically")
    with psycopg.connect(dsn, autocommit=True) as conn:
        version = migrate(
            conn, args.schema, target=args.target, allow_downgrade=args.allow_downgrade
        )
    print(f"Metadata schema is at version {version}")


if __name__ == "__main__":
    main()
