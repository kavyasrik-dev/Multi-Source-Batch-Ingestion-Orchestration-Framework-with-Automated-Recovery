"""Real PostgreSQL incremental extraction and guarded watermark integration test."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from app.common.metadata.migrate import migrate
from app.common.metadata.repository import MetadataRepository
from app.connectors.postgres import PostgresAdapter, PostgresAdapterConfig, PsycopgSourceReader

pytestmark = pytest.mark.integration


def test_incremental_windows_late_rows_and_safe_watermark_advancement():
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("Set TEST_DATABASE_URL to run real PostgreSQL integration tests")
    suffix = uuid4().hex
    metadata_schema = "test_metadata_" + suffix
    source_schema = "test_source_" + suffix
    with psycopg.connect(dsn, autocommit=True) as setup:
        try:
            migrate(setup, metadata_schema)
            setup.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(source_schema)))
            setup.execute(
                sql.SQL(
                    """CREATE TABLE {}.orders (
                    id bigint PRIMARY KEY,
                    updated_at timestamptz NOT NULL,
                    amount text NOT NULL
                    )"""
                ).format(sql.Identifier(source_schema))
            )
            base = datetime(2026, 11, 23, 12, tzinfo=UTC)
            setup.executemany(
                sql.SQL("INSERT INTO {}.orders VALUES (%s, %s, %s)").format(
                    sql.Identifier(source_schema)
                ),
                [(1, base, "10"), (2, base, "20"), (3, base + timedelta(minutes=1), "30")],
            )
            repository = MetadataRepository(setup, metadata_schema)
            source_config = repository.create_source("orders-db", "postgres")
            config = PostgresAdapterConfig(
                source_id=source_config["id"],
                source_name="orders-db",
                schema=source_schema,
                table="orders",
                columns=("id", "updated_at", "amount"),
                watermark_column="updated_at",
                tie_breaker_column="id",
                lookback_seconds=3600,
            )
            reader = PsycopgSourceReader(lambda: psycopg.connect(dsn))
            first_adapter = PostgresAdapter(config, reader, repository)
            [source] = first_adapter.discover()
            first = first_adapter.extract_window(source)
            assert [record.values["id"] for record in first.records] == [1, 2, 3]
            first_adapter.acknowledge_published(
                first, validation_passed=True, publication_uri="s3://curated/first.parquet"
            )

            setup.executemany(
                sql.SQL("INSERT INTO {}.orders VALUES (%s, %s, %s)").format(
                    sql.Identifier(source_schema)
                ),
                [
                    (99, base + timedelta(seconds=30), "late"),
                    (4, base + timedelta(minutes=1), "40"),
                ],
            )
            second_adapter = PostgresAdapter(config, reader, repository)
            second = second_adapter.extract_window(source)
            identifiers = [record.values["id"] for record in second.records]
            assert 99 in identifiers
            assert identifiers[-2:] == [3, 4]
            before = repository.load_source_watermark(source_config["id"])
            assert before["tie_breaker"] == 3
            second_adapter.acknowledge_published(
                second, validation_passed=True, publication_uri="s3://curated/second.parquet"
            )
            after = repository.load_source_watermark(source_config["id"])
            assert after["tie_breaker"] == 4
            assert after["version"] == before["version"] + 1
        finally:
            setup.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(source_schema))
            )
            setup.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(metadata_schema))
            )
