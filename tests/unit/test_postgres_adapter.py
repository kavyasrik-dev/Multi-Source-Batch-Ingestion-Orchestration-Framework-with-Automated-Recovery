"""Incremental PostgreSQL adapter and publication-gated watermark tests."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest

from app.connectors import SourceAdapter
from app.connectors.postgres import (
    PostgresAdapter,
    PostgresAdapterConfig,
    PostgresConnectionError,
    PostgresRowError,
    PsycopgSourceReader,
    WatermarkAdvanceError,
)

BASE_TIME = datetime(2026, 11, 23, 12, tzinfo=UTC)


class FakeReader:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def read(self, config, state):
        self.calls.append((config, state))
        return self.rows


class FakeWatermarks:
    def __init__(self, watermark=None, tie_breaker=None, version=0):
        self.row = {
            "watermark": watermark,
            "tie_breaker": tie_breaker,
            "version": version,
        }
        self.advances = []
        self.fail_advance = False

    def load_source_watermark(self, source_id):
        return dict(self.row)

    def advance_source_watermark(self, source_id, **values):
        if self.fail_advance:
            raise ValueError("stale")
        self.advances.append((source_id, values))
        self.row = {
            "watermark": values["watermark"],
            "tie_breaker": values["tie_breaker"],
            "version": values["expected_version"] + 1,
            "publication_uri": values["publication_uri"],
        }
        return dict(self.row)


def config(**changes):
    values = {
        "source_id": uuid4(),
        "source_name": "orders-db",
        "schema": "source_data",
        "table": "orders",
        "columns": ("id", "updated_at", "amount"),
        "watermark_column": "updated_at",
        "tie_breaker_column": "id",
        "tie_breaker_type": "integer",
        "lookback_seconds": 3600,
        "max_window_records": 100,
    }
    values.update(changes)
    return PostgresAdapterConfig(**values)


def row(identifier, timestamp, amount="10.00"):
    return {"id": identifier, "updated_at": timestamp, "amount": amount}


def test_null_watermark_extracts_initial_window_with_equal_timestamps_and_stable_ids():
    rows = [row(1, BASE_TIME), row(2, BASE_TIME), row(3, BASE_TIME + timedelta(seconds=1))]
    reader = FakeReader(rows)
    watermarks = FakeWatermarks()
    adapter = PostgresAdapter(config(), reader, watermarks)
    assert isinstance(adapter, SourceAdapter)
    [source] = adapter.discover()
    first = list(adapter.iter_batches(source, max_records=2))
    replay = list(adapter.iter_batches(source, max_records=2))
    assert [len(batch.records) for batch in first] == [2, 1]
    assert [record.values["id"] for batch in first for record in batch.records] == [1, 2, 3]
    assert [batch.batch_id for batch in first] == [batch.batch_id for batch in replay]
    assert len(reader.calls) == 1
    assert not watermarks.advances
    window = adapter.extract_window(source)
    assert (window.candidate_watermark, window.candidate_tie_breaker) == (
        BASE_TIME + timedelta(seconds=1),
        3,
    )


def test_watermark_advances_only_after_validation_and_publication():
    watermarks = FakeWatermarks()
    adapter = PostgresAdapter(config(), FakeReader([row(1, BASE_TIME)]), watermarks)
    [source] = adapter.discover()
    window = adapter.extract_window(source)
    with pytest.raises(WatermarkAdvanceError, match="validation must pass"):
        adapter.acknowledge_published(
            window, validation_passed=False, publication_uri="s3://curated/orders.parquet"
        )
    with pytest.raises(WatermarkAdvanceError, match="publication URI"):
        adapter.acknowledge_published(window, validation_passed=True, publication_uri="")
    assert not watermarks.advances
    result = adapter.acknowledge_published(
        window,
        validation_passed=True,
        publication_uri="s3://curated/orders.parquet",
    )
    assert result["version"] == 1
    assert watermarks.advances[0][1]["expected_version"] == 0


def test_late_rows_are_replayed_and_new_equal_timestamp_tie_breaker_is_not_skipped():
    state_time = BASE_TIME + timedelta(hours=2)
    rows = [
        row(99, BASE_TIME + timedelta(hours=1, minutes=30)),
        row(8, state_time),
        row(1, state_time + timedelta(minutes=1)),
    ]
    watermarks = FakeWatermarks(state_time, 7, version=4)
    adapter = PostgresAdapter(config(), FakeReader(rows), watermarks)
    [source] = adapter.discover()
    window = adapter.extract_window(source)
    assert [record.values["id"] for record in window.records] == [99, 8, 1]
    assert window.base_state.tie_breaker == 7
    assert (window.candidate_watermark, window.candidate_tie_breaker) == (
        state_time + timedelta(minutes=1),
        1,
    )


def test_overlap_only_window_is_publishable_without_regressing_watermark():
    state_time = BASE_TIME + timedelta(hours=2)
    watermarks = FakeWatermarks(state_time, 7, version=2)
    adapter = PostgresAdapter(
        config(),
        FakeReader([row(99, state_time - timedelta(minutes=30))]),
        watermarks,
    )
    [source] = adapter.discover()
    window = adapter.extract_window(source)
    assert (
        adapter.acknowledge_published(
            window, validation_passed=True, publication_uri="s3://curated/replay.parquet"
        )
        is None
    )
    assert not watermarks.advances


def test_concurrent_watermark_change_is_reported_safely():
    watermarks = FakeWatermarks()
    watermarks.fail_advance = True
    adapter = PostgresAdapter(config(), FakeReader([row(1, BASE_TIME)]), watermarks)
    [source] = adapter.discover()
    window = adapter.extract_window(source)
    with pytest.raises(WatermarkAdvanceError, match="concurrently"):
        adapter.acknowledge_published(
            window, validation_passed=True, publication_uri="s3://curated/orders.parquet"
        )


@pytest.mark.parametrize(
    "rows, message",
    [
        ([row(2, BASE_TIME), row(1, BASE_TIME)], "strictly deterministic"),
        ([row(1, None)], "non-null timestamptz"),
        ([row("one", BASE_TIME)], "tie-breaker must be integer"),
        ([{"id": 1, "updated_at": BASE_TIME}], "expected columns"),
    ],
)
def test_invalid_source_rows_are_actionable_and_never_advance(rows, message):
    watermarks = FakeWatermarks()
    adapter = PostgresAdapter(config(), FakeReader(rows), watermarks)
    [source] = adapter.discover()
    with pytest.raises(PostgresRowError, match=message):
        list(adapter.extract(source))
    assert not watermarks.advances


def test_connection_loss_is_wrapped_without_watermark_change():
    watermarks = FakeWatermarks()

    def disconnected():
        raise psycopg.OperationalError("connection lost")

    adapter = PostgresAdapter(config(), PsycopgSourceReader(disconnected), watermarks)
    [source] = adapter.discover()
    with pytest.raises(PostgresConnectionError, match="read-only extraction failed"):
        list(adapter.extract(source))
    assert not watermarks.advances


def test_empty_initial_window_has_stable_metadata_and_no_advancement():
    watermarks = FakeWatermarks()
    adapter = PostgresAdapter(config(), FakeReader([]), watermarks)
    [source] = adapter.discover()
    window = adapter.extract_window(source)
    assert window.records == ()
    assert list(adapter.iter_batches(source, max_records=10)) == []
    assert (
        adapter.acknowledge_published(
            window, validation_passed=True, publication_uri="s3://curated/empty.json"
        )
        is None
    )
    assert not watermarks.advances


def test_configuration_requires_incremental_columns_and_valid_bounds():
    with pytest.raises(ValueError, match="include watermark"):
        config(columns=("id", "amount"))
    with pytest.raises(ValueError, match="lookback_seconds"):
        config(lookback_seconds=-1)
    with pytest.raises(ValueError, match="max_window_records"):
        config(max_window_records=0)


@pytest.mark.parametrize("max_records", [0, -1, True, 1.5])
def test_invalid_batch_bounds_are_rejected(max_records):
    adapter = PostgresAdapter(config(), FakeReader([]), FakeWatermarks())
    [source] = adapter.discover()
    with pytest.raises(ValueError, match="positive integer"):
        list(adapter.iter_batches(source, max_records=max_records))
